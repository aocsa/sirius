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

/**
 * @file bloom_sizing.hpp
 * @brief Bloom filter sizing shared by `sirius_dynamic_bloom_filter`, built from a whole build, and
 * `detail::accumulated_bloom_geometry`, accumulated from a multi-partition build, so that retuning
 * the bits per key changes both.
 */

#pragma once

#include <rmm/aligned.hpp>

#include <cuda/cmath>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace sirius::op {

inline constexpr std::size_t bloom_bits_per_block      = 256;  ///< One cuco Bloom filter block
inline constexpr std::size_t bloom_bytes_per_block     = bloom_bits_per_block / 8;
inline constexpr std::size_t bloom_target_bits_per_key = 16;

static_assert(bloom_bits_per_block % bloom_target_bits_per_key == 0,
              "a block must hold a whole number of keys");

/**
 * @brief Number of Bloom blocks for @p num_keys keys: at least one, and never overflowing.
 */
[[nodiscard]] constexpr std::size_t bloom_blocks_for(std::size_t num_keys) noexcept
{
  return ::cuda::ceil_div(std::max<std::size_t>(num_keys, 1),
                          bloom_bits_per_block / bloom_target_bits_per_key);
}

/**
 * @brief The most keys each of @p key_count Blooms can be sized for when together they must fit in
 * @p cap: the cap split evenly per key, rounded down to the allocation alignment, as whole blocks
 * of bloom_bytes_per_block, each holding bloom_bits_per_block / bloom_target_bits_per_key keys.
 *
 * The inverse of detail::accumulated_bloom_geometry::try_create's footprint; change both together.
 */
[[nodiscard]] constexpr std::size_t bloom_keys_within_cap(std::uint64_t cap,
                                                          std::size_t key_count) noexcept
{
  auto constexpr alignment = rmm::CUDA_ALLOCATION_ALIGNMENT;
  auto const blocks        = cap / key_count / alignment * alignment / bloom_bytes_per_block;
  return blocks * (bloom_bits_per_block / bloom_target_bits_per_key);
}

/**
 * @brief The keys a distributed merged Bloom is sized for: this rank's @p local_rows times
 * @p world, since a hash-exchanged build holds about a world-th of the keys on each rank; at least
 * @p min_keys, at most @p max_keys; rounded up to a power-of-two number of blocks.
 *
 * Every rank sizes its own Bloom, so ranks whose rows straddle a power of two get different sizes.
 * Power-of-two block counts let the merge fold the larger Blooms onto the smallest
 * (sirius_dynamic_bloom_filter::merge_from). @p max_keys is taken down to a power-of-two number of
 * blocks, so the result never exceeds it.
 */
[[nodiscard]] constexpr std::size_t distributed_bloom_keys(std::size_t local_rows,
                                                           std::size_t world,
                                                           std::size_t min_keys,
                                                           std::size_t max_keys) noexcept
{
  constexpr std::size_t keys_per_block = bloom_bits_per_block / bloom_target_bits_per_key;
  auto const max_blocks = std::bit_floor(std::max<std::size_t>(max_keys / keys_per_block, 1));
  auto const ranks      = std::max<std::size_t>(world, 1);
  auto const rows       = local_rows > std::numeric_limits<std::size_t>::max() / ranks
                            ? std::numeric_limits<std::size_t>::max()
                            : local_rows * ranks;
  auto const blocks     = bloom_blocks_for(std::max(rows, min_keys));
  return (blocks >= max_blocks ? max_blocks : std::bit_ceil(blocks)) * keys_per_block;
}

}  // namespace sirius::op
