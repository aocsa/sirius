/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "op/sirius_physical_distributed_sort_sample.hpp"

#include "data/data_batch_utils.hpp"
#include "exchange/all_gather.hpp"
#include "exchange/exchange_service.hpp"
#include "op/cudf_sort_order.hpp"
#include "op/merge/gpu_merge_impl.hpp"
#include "pipeline/sirius_pipeline.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>
#include <cudf/search.hpp>

#include <algorithm>
#include <numeric>

namespace sirius::op {

namespace {

/// A device INT32 column holding @p indices.
std::unique_ptr<cudf::column> index_column(std::vector<int32_t> const& indices,
                                           ::cuda::stream_ref stream,
                                           rmm::device_async_resource_ref mr)
{
  auto column = cudf::make_numeric_column(cudf::data_type{cudf::type_id::INT32},
                                          static_cast<cudf::size_type>(indices.size()),
                                          cudf::mask_state::UNALLOCATED,
                                          stream,
                                          mr);
  CUDF_CUDA_TRY(cudaMemcpyAsync(column->mutable_view().data<int32_t>(),
                                indices.data(),
                                indices.size() * sizeof(int32_t),
                                cudaMemcpyHostToDevice,
                                stream.get()));
  return column;
}

/// Rows i * rows / parts of @p sorted for i in [1, parts), offset by @p first: parts - 1
/// evenly spaced boundaries.
std::unique_ptr<cudf::table> evenly_spaced(cudf::table_view sorted,
                                           cudf::size_type first,
                                           cudf::size_type rows,
                                           std::size_t parts,
                                           ::cuda::stream_ref stream,
                                           rmm::device_async_resource_ref mr)
{
  std::vector<int32_t> indices;
  for (std::size_t i = 1; i < parts; ++i) {
    auto const at = static_cast<std::size_t>(rows) * i / parts;
    indices.push_back(first + static_cast<int32_t>(std::min<std::size_t>(at, rows - 1)));
  }
  auto const column = index_column(indices, stream, mr);
  return cudf::gather(sorted, column->view(), cudf::out_of_bounds_policy::DONT_CHECK, stream, mr);
}

}  // namespace

sirius_physical_distributed_sort_sample::sirius_physical_distributed_sort_sample(
  sirius_physical_sort_sample const& local, distributed_sampling sampling)
  : sirius_physical_sort_sample(local.types,
                                copy_orders(local.orders),
                                local.estimated_cardinality,
                                local.sort_sample_bytes(),
                                local.max_partition_bytes(),
                                local.max_partition_memory_fraction()),
    _sampling(std::move(sampling))
{
  for (auto const& ord : orders) {
    _column_order.push_back(to_cudf_order(ord.type));
    _null_precedence.push_back(to_cudf_null_order(ord.type, ord.null_order));
  }
}

std::unique_ptr<operator_data> sirius_physical_distributed_sort_sample::execute(
  const operator_data& input_data, ::cuda::stream_ref stream)
{
  // The base computes no boundaries from an empty sample, but this rank must still join the
  // all-gather the others wait in.
  auto const& input = dynamic_cast<const pipelineable_operator_data&>(input_data);
  if (!boundaries_computed() && input.get_read_only_batches().empty()) {
    compute_boundaries({}, 1, *_sampling.gpu, stream);
  }
  return sirius_physical_sort_sample::execute(input_data, stream);
}

std::unique_ptr<operator_data> sirius_physical_distributed_sort_sample::get_next_task_input_data()
{
  auto input = sirius_physical_sort_sample::get_next_task_input_data();
  if (input || _boundary_state.load(std::memory_order_acquire) != BoundaryState::NOT_DONE) {
    return input;
  }
  // A rank with no input still joins the boundary exchange the others wait in.
  auto port_ids = get_port_ids();
  if (port_ids.empty()) { return nullptr; }
  auto* p = get_port(port_ids[0]);
  if (!p || !p->repo) { return nullptr; }
  bool const upstream_finished = p->src_pipeline && p->src_pipeline->is_pipeline_finished();
  if (!upstream_finished) { return nullptr; }

  _boundary_state.store(BoundaryState::SCHEDULED, std::memory_order_release);
  return std::make_unique<pipelineable_operator_data>(
    std::vector<std::shared_ptr<::cucascade::data_batch>>{});
}

bool sirius_physical_distributed_sort_sample::all_ports_empty()
{
  if (_boundary_state.load(std::memory_order_acquire) == BoundaryState::NOT_DONE) { return false; }
  return sirius_physical_sort_sample::all_ports_empty();
}

void sirius_physical_distributed_sort_sample::compute_boundaries(cudf::table_view sample_keys,
                                                                 size_t num_parts,
                                                                 cucascade::memory::memory_space&,
                                                                 ::cuda::stream_ref stream)
{
  auto& gpu        = *_sampling.gpu;
  auto const mr    = gpu.get_default_allocator();
  auto const world = _sampling.service->world_size();
  auto const rank  = _sampling.service->rank();

  // 1. This rank's summary: up to distributed_summary_rows evenly spaced rows of its sample.
  std::shared_ptr<::cucascade::data_batch> summary;
  if (auto const rows = sample_keys.num_rows(); rows > 0) {
    auto const take = std::min<std::size_t>(rows, distributed_summary_rows);
    std::vector<int32_t> indices(take);
    for (std::size_t i = 0; i < take; ++i) {
      indices[i] = static_cast<int32_t>(i * static_cast<std::size_t>(rows) / take);
    }
    auto const column = index_column(indices, stream, mr);
    summary           = make_data_batch(
      cudf::gather(sample_keys, column->view(), cudf::out_of_bounds_policy::DONT_CHECK, stream, mr),
      gpu,
      stream,
      batch_telemetry());
  }

  // 2. Every rank's summary, merged in rank order: the same global sample on every rank.
  // A retried task reuses the result: the peers have closed the all-gather's stream.
  if (!_gathered) {
    _gathered =
      exchange::all_gather(*_sampling.service, _sampling.stream, summary, gpu, stream.get());
  }
  std::vector<::cucascade::read_only_data_batch> parts;
  for (auto const& g : *_gathered) {
    if (g) { parts.push_back(g->to_read_only()); }
  }
  _num_partitions = 1;
  _partition_boundaries.reset();
  if (parts.empty()) { return; }
  std::vector<int> keys(_column_order.size());
  std::iota(keys.begin(), keys.end(), 0);
  std::shared_ptr<::cucascade::data_batch> merged;
  if (parts.size() > 1) {
    merged = gpu_merge_impl::merge_order_by(
      parts, keys, _column_order, _null_precedence, stream, gpu, batch_telemetry());
  }
  auto const global =
    merged ? get_cudf_table_view(merged->to_read_only()) : get_cudf_table_view(parts.front());
  auto const total = global.num_rows();

  // 3. The range exchange's world - 1 boundaries, evenly spaced over the global sample.
  cudf::size_type lo = 0;
  cudf::size_type hi = total;
  if (world > 1) {
    std::shared_ptr<cudf::table const> exchange_boundaries =
      evenly_spaced(global, 0, total, world, stream, mr);
    // This rank's range of the global sample: from the first row not below its lower boundary
    // to the first not below its upper one, as the exchange routes rows.
    auto const starts = cudf::lower_bound(
      global, exchange_boundaries->view(), _column_order, _null_precedence, stream, mr);
    std::vector<cudf::size_type> at(world - 1);
    CUDF_CUDA_TRY(cudaMemcpyAsync(at.data(),
                                  starts->view().data<cudf::size_type>(),
                                  at.size() * sizeof(cudf::size_type),
                                  cudaMemcpyDeviceToHost,
                                  stream.get()));
    CUDF_CUDA_TRY(cudaStreamSynchronize(stream.get()));
    if (rank > 0) { lo = at[rank - 1]; }
    if (rank + 1 < world) { hi = at[rank]; }
    _sampling.set_exchange_boundaries(std::move(exchange_boundaries));
  }

  // 4. This rank's partition boundaries, from the part of the global sample inside its range.
  auto const parts_here = std::min<std::size_t>(num_parts, std::max<cudf::size_type>(1, hi - lo));
  if (parts_here <= 1) { return; }
  _partition_boundaries = evenly_spaced(global, lo, hi - lo, parts_here, stream, mr);
  _num_partitions       = parts_here;
}

}  // namespace sirius::op
