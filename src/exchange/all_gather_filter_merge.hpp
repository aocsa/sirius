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

#pragma once

#include "exchange/transport.hpp"
#include "op/dynamic_filter/cross_rank_filter_merge.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

namespace cucascade::memory {
class memory_space;
}

namespace sirius::exchange {

class exchange_service;

/// Merges the ranks' Blooms with one all-gather per admitted key.
class all_gather_filter_merge final : public op::cross_rank_filter_merge {
 public:
  struct options {
    std::shared_ptr<exchange_service> service;
    /// Admitted key i merges on stream first_stream + i, the same on every rank.
    stream_id first_stream               = 0;
    cucascade::memory::memory_space* gpu = nullptr;
    /// Every rank's Bloom is sized for this many keys.
    std::size_t bloom_keys = 0;
    /// A rank that has not sent its filter by then is given up on: the key publishes nothing.
    std::chrono::milliseconds timeout{std::chrono::minutes{2}};
  };

  explicit all_gather_filter_merge(options options) : _options(std::move(options)) {}

  [[nodiscard]] std::size_t bloom_keys() const noexcept override { return _options.bloom_keys; }
  void merge(std::vector<op::key_filters>& per_key, ::cuda::stream_ref stream) override;
  void leave(std::size_t keys) noexcept override;

 private:
  void leave_keys(std::size_t first, std::size_t last) noexcept;

  options _options;
  /// This rank has taken its part in every key's merge, with filters or without.
  std::atomic<bool> _took_part{false};
};

}  // namespace sirius::exchange
