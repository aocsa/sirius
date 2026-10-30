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

#include "exchange/all_gather_filter_merge.hpp"

#include "data/data_batch_utils.hpp"
#include "exchange/all_gather.hpp"
#include "exchange/exchange_service.hpp"
#include "log/logging.hpp"
#include "op/dynamic_filter/sirius_dynamic_filter.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/table/table.hpp>

#include <rmm/error.hpp>

#include <cuda_runtime_api.h>

#include <absl/cleanup/cleanup.h>
#include <cucascade/data/data_batch.hpp>
#include <cucascade/memory/memory_space.hpp>

#include <array>
#include <cstdint>
#include <exception>

namespace sirius::exchange {

/**
 * ORs every rank's Bloom for each admitted key into this rank's, so it holds every rank's build
 * keys, or drops the key's filter unless every rank sent one of the same size and key domain. Every
 * rank calls it for every admitted key, in order, filter or not, since the others wait for it.
 */
void all_gather_filter_merge::merge(std::vector<op::key_filters>& per_key,
                                    ::cuda::stream_ref stream)
{
  _took_part    = true;
  auto const& d = _options;
  // The first key whose all-gather this rank has not joined. If anything throws, the other ranks
  // would wait on this rank's part in the rest until the timeout; after the last key, none is left.
  std::size_t next_key           = 0;
  absl::Cleanup const leave_rest = [&] { leave_keys(next_key, per_key.size()); };
  // A rank's contribution: rep | family | scale | storage bytes | the Bloom's words, as UINT32.
  constexpr std::size_t header = 4;
  auto const mr                = d.gpu->get_default_allocator();
  for (std::size_t k = 0; k < per_key.size(); ++k) {
    auto& membership = per_key[k].membership;
    auto* bloom      = dynamic_cast<op::sirius_dynamic_bloom_filter*>(membership.get());
    std::array<std::uint32_t, header> head{};
    std::shared_ptr<cucascade::data_batch> mine;
    if (bloom) {
      auto const bytes = bloom->storage_bytes();
      head             = {static_cast<std::uint32_t>(bloom->domain().rep),
                          static_cast<std::uint32_t>(bloom->domain().family),
                          static_cast<std::uint32_t>(bloom->domain().scale),
                          static_cast<std::uint32_t>(bytes)};
      auto column      = cudf::make_numeric_column(cudf::data_type{cudf::type_id::UINT32},
                                              static_cast<cudf::size_type>(header + bytes / 4),
                                              cudf::mask_state::UNALLOCATED,
                                              stream,
                                              mr);
      auto* words      = column->mutable_view().data<std::uint32_t>();
      RMM_CUDA_TRY(
        cudaMemcpyAsync(words, head.data(), sizeof(head), cudaMemcpyHostToDevice, stream.get()));
      RMM_CUDA_TRY(cudaMemcpyAsync(
        words + header, bloom->storage(), bytes, cudaMemcpyDeviceToDevice, stream.get()));
      std::vector<std::unique_ptr<cudf::column>> columns;
      columns.push_back(std::move(column));
      mine = sirius::make_data_batch(
        std::make_unique<cudf::table>(std::move(columns)), *d.gpu, stream, {});
    }
    std::vector<std::shared_ptr<cucascade::data_batch>> all;
    next_key = k + 1;
    try {
      all = exchange::all_gather(
        *d.service, d.first_stream + k, std::move(mine), *d.gpu, stream.get(), d.timeout);
    } catch (std::exception const& e) {
      SIRIUS_LOG_WARN(
        "[publish_dynamic_filters] key {}: filters not merged across ranks: {}", k, e.what());
      membership.reset();
      continue;
    }
    if (!bloom) { continue; }
    std::vector<std::uint32_t const*> others;
    bool mergeable = true;
    for (exchange::rank_t r = 0; r < all.size() && mergeable; ++r) {
      if (!all[r]) {
        mergeable = false;
        break;
      }
      auto const* theirs = sirius::get_cudf_table_view(*all[r]).column(0).data<std::uint32_t>();
      std::array<std::uint32_t, header> their_head{};
      RMM_CUDA_TRY(cudaMemcpyAsync(
        their_head.data(), theirs, sizeof(their_head), cudaMemcpyDeviceToHost, stream.get()));
      stream.sync();
      mergeable = their_head == head;
      if (r != d.service->rank()) { others.push_back(theirs + header); }
    }
    if (!mergeable) {
      SIRIUS_LOG_DEBUG(
        "[publish_dynamic_filters] key {}: a rank sent no filter or another kind; "
        "nothing published for it.",
        k);
      membership.reset();
      continue;
    }
    for (auto const* words : others) {
      bloom->merge_from(words, bloom->storage_bytes(), stream);
    }
    // The gathered filters are freed with `all`, after the merges have read them.
    stream.sync();
  }
}

/// Lets the other ranks drop each key's filter at once instead of at the merge timeout.
void all_gather_filter_merge::leave(std::size_t keys) noexcept
{
  if (_took_part.exchange(true)) { return; }
  leave_keys(0, keys);
}

void all_gather_filter_merge::leave_keys(std::size_t first, std::size_t last) noexcept
{
  try {
    for (std::size_t k = first; k < last; ++k) {
      exchange::leave_all_gather(*_options.service, _options.first_stream + k);
    }
  } catch (std::exception const& e) {
    SIRIUS_LOG_WARN("[publish_dynamic_filters] taking part in the merge without filters: {}",
                    e.what());
  }
}

}  // namespace sirius::exchange
