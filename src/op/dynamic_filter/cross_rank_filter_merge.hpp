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

#include "op/dynamic_filter/dynamic_filter_key_domain.hpp"

#include <cudf/types.hpp>

#include <cuda/stream>

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace sirius::op {

class sirius_dynamic_filter;

/**
 * @brief The filters one admitted key publishes; a null filter publishes nothing.
 */
struct key_filters {
  std::shared_ptr<sirius_dynamic_filter> zone_map;
  /// The type the zone map bounds carry: the key's recorded storage type.
  cudf::data_type zone_map_type{cudf::type_id::EMPTY};
  std::shared_ptr<sirius_dynamic_filter> membership;
  /// The domain the membership filter was built over.
  std::optional<membership_key_domain> membership_domain;
};

/**
 * @brief Distributed execution: after an exchange, a rank's build holds only its share of the
 * keys, so a filter from it alone would drop rows another rank's build matches. Each rank
 * instead builds a Bloom of the same size per key, and every rank ORs them all. A key whose
 * filters cannot all be merged publishes nothing; other filter kinds are not published.
 */
class cross_rank_filter_merge {
 public:
  virtual ~cross_rank_filter_merge() = default;

  /// Every rank's Bloom is sized for this many keys.
  [[nodiscard]] virtual std::size_t bloom_keys() const noexcept = 0;

  /// Replaces each admitted key's Bloom with the OR of every rank's, or drops it.
  virtual void merge(std::vector<key_filters>& per_key, ::cuda::stream_ref stream) = 0;

  /**
   * @brief Takes this rank's part in the merge of each of @p keys admitted keys without filters.
   * Does nothing once this rank has taken part.
   *
   * Must not wait for the other ranks: the caller finalizes a pipeline, and may be the exchange's
   * engine thread delivering an end of stream, which a waiting merge would itself need to move its
   * messages.
   */
  virtual void leave(std::size_t keys) noexcept = 0;
};

}  // namespace sirius::op
