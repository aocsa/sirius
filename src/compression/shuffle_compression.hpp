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

#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cuda/stream_ref>
#include <rmm/resource_ref.hpp>

#include <cstddef>
#include <memory>

/**
 * @file
 * @brief Simpatico compression of the batches an exchange sends a peer.
 *
 * A compressed batch travels as an ordinary batch of one UINT64 column whose bytes are a magic
 * number, the Simpatico header's size, the payload's size, the header, and at the next 256-byte
 * boundary the payload. The transport and the wire format are unchanged; EXCHANGE_SINK packs and
 * EXCHANGE_SOURCE unpacks. A stream whose own schema is exactly one UINT64 column is never packed,
 * so a received batch of that shape on any other stream is always a packed one.
 */
namespace sirius::compression {

/// SIRIUS_EXCHANGE_COMPRESSION: off (default), staged (only to peers the fabric host-stages, i.e.
/// without peer access) or all (every peer).
enum class shuffle_compression { off, staged, all };

/// The mode from the environment, read once.
/// @throws sirius::invalid_input_exception on a value other than off, staged or all.
[[nodiscard]] shuffle_compression shuffle_compression_mode();

/// Whether @p table has the shape a packed batch travels as: exactly one UINT64 column.
[[nodiscard]] bool is_shuffle_blob_schema(cudf::table_view const& table);

/// What packing saved, for the log.
struct shuffle_compression_stats {
  std::size_t raw_bytes{0};
  std::size_t packed_bytes{0};
};

/**
 * @brief Packs @p table for a peer: Simpatico bitpack (frame of reference, then bit packing) for
 * its INT32, INT64, TIMESTAMP_DAYS and DECIMAL64 columns, every other column stored as is.
 *
 * Synchronizes @p stream before returning.
 * @return nullptr when no column can be bitpacked, @p table has no rows, or the packed batch would
 *         not be at least 10% smaller; @p stats is set either way.
 */
[[nodiscard]] std::unique_ptr<cudf::table> compress_for_shuffle(
  cudf::table_view table,
  ::cuda::stream_ref stream,
  rmm::device_async_resource_ref mr,
  shuffle_compression_stats& stats);

/// Rebuilds the table compress_for_shuffle() packed into @p blob. Synchronizes @p stream.
/// @throws sirius::internal_exception when @p blob is not a packed batch.
[[nodiscard]] std::unique_ptr<cudf::table> decompress_from_shuffle(
  cudf::table_view blob, ::cuda::stream_ref stream, rmm::device_async_resource_ref mr);

}  // namespace sirius::compression
