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

#include "exchange/stage_choice.hpp"

#include "data/data_batch_utils.hpp"
#include "exchange/all_gather.hpp"
#include "exchange/exchange_service.hpp"
#include "sirius/exception.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/table/table.hpp>
#include <cudf/utilities/error.hpp>

#include <rmm/cuda_device.hpp>

#include <cucascade/data/data_batch.hpp>
#include <cucascade/memory/memory_space.hpp>

#include <array>
#include <limits>
#include <utility>
#include <vector>

namespace sirius::exchange {
namespace {

std::uint64_t saturating_add(std::uint64_t a, std::uint64_t b) noexcept
{
  return b > std::numeric_limits<std::uint64_t>::max() - a ? std::numeric_limits<std::uint64_t>::max()
                                                           : a + b;
}

}  // namespace

stage_count::stage_count(std::shared_ptr<exchange_service> service,
                         stream_id stream,
                         cucascade::memory::memory_space& gpu,
                         std::chrono::milliseconds timeout)
  : _service(std::move(service)), _stream(stream), _gpu(&gpu), _timeout(timeout)
{
  if (!_service) { throw sirius::invalid_input_exception("stage count: no exchange service"); }
}

void stage_count::over()
{
  {
    std::lock_guard lock{_mutex};
    if (_state != state::open) { return; }
    _state = state::resolved;
  }
  _changed.notify_all();
  leave_all_gather(*_service, _stream);
}

std::optional<stage_load> stage_count::total(stage_load mine, ::cuda::stream_ref stream)
{
  {
    std::unique_lock lock{_mutex};
    if (_state != state::open) { return answer(lock); }
    _state = state::gathering;
  }
  std::optional<stage_load> sum;
  std::exception_ptr error;
  try {
    sum = gather(mine, stream);
  } catch (...) {
    error = std::current_exception();
  }
  std::unique_lock lock{_mutex};
  _sum   = sum;
  _error = error;
  _state = state::resolved;
  _changed.notify_all();
  return answer(lock);
}

bool stage_count::resolved() const
{
  std::lock_guard lock{_mutex};
  return _state == state::resolved;
}

std::optional<stage_load> stage_count::wait() const
{
  std::unique_lock lock{_mutex};
  return answer(lock);
}

std::optional<stage_load> stage_count::answer(std::unique_lock<std::mutex>& lock) const
{
  if (!_changed.wait_for(lock, _timeout, [this] { return _state == state::resolved; })) {
    throw sirius::internal_exception(
      "stage count on stream {}: no answer in {} ms", _stream, _timeout.count());
  }
  if (_error) { std::rethrow_exception(_error); }
  return _sum;
}

std::optional<stage_load> stage_count::gather(stage_load mine, ::cuda::stream_ref stream)
{
  // The caller may be a thread of its own, not one of the GPU's task threads.
  rmm::cuda_set_device_raii const device{rmm::cuda_device_id{_gpu->get_device_id()}};
  auto const mr = _gpu->get_default_allocator();
  // One row: bytes, rows.
  std::vector<std::unique_ptr<cudf::column>> columns;
  for (auto const count : {mine.bytes, mine.rows}) {
    cudf::numeric_scalar<std::int64_t> const value{
      static_cast<std::int64_t>(count), true, stream, mr};
    columns.push_back(cudf::make_column_from_scalar(value, 1, stream, mr));
  }
  auto batch =
    make_data_batch(std::make_unique<cudf::table>(std::move(columns)), *_gpu, stream, {});
  auto const counts =
    all_gather(*_service, _stream, std::move(batch), *_gpu, stream.get(), _timeout);

  // A rank that went over left no table.
  std::vector<std::array<std::int64_t, 2>> theirs(counts.size());
  for (std::size_t r = 0; r < counts.size(); ++r) {
    if (!counts[r]) { return std::nullopt; }
    auto const view = get_cudf_table_view(*counts[r]);
    for (cudf::size_type c = 0; c < 2; ++c) {
      CUDF_CUDA_TRY(cudaMemcpyAsync(&theirs[r][c],
                                    view.column(c).data<std::int64_t>(),
                                    sizeof(std::int64_t),
                                    cudaMemcpyDeviceToHost,
                                    stream.get()));
    }
  }
  CUDF_CUDA_TRY(cudaStreamSynchronize(stream.get()));
  stage_load sum;
  for (auto const& held : theirs) {
    sum.bytes = saturating_add(sum.bytes, static_cast<std::uint64_t>(held[0]));
    sum.rows  = saturating_add(sum.rows, static_cast<std::uint64_t>(held[1]));
  }
  return sum;
}

broadcast_choice::broadcast_choice(std::shared_ptr<exchange_service> service,
                                   stream_id stream,
                                   std::uint64_t limit_bytes,
                                   cucascade::memory::memory_space& gpu,
                                   std::chrono::milliseconds timeout)
  : _count(service, stream, gpu, timeout),
    _limit(limit_bytes),
    _share(limit_bytes / service->world_size())
{
}

std::optional<bool> broadcast_choice::known() const
{
  if (!_count.resolved()) { return std::nullopt; }
  return settled(_count.wait());
}

bool broadcast_choice::settled(std::optional<stage_load> const& sum) const
{
  std::call_once(_weighed, [&] {
    _whole = sum && sum->bytes <= _limit && (!_worth || _worth(*sum));
  });
  return _whole;
}

}  // namespace sirius::exchange
