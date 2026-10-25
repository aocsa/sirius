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

// Partial pin serving: a parquet scan whose pin holds only some of its columns reads those from
// the pin and the rest of the same rows from disk. A pinned chunk's rows are exactly the row
// groups recorded in pinned_entry::chunk_sources, so each chunk pairs with one unfiltered disk
// read of those row groups; the scan's filter runs afterwards, on the joined batch.

#pragma once

#include "scan_manager/load_balancing_scan_batch_coalescer.hpp"
#include "scan_manager/sirius_scan_manager.hpp"

#include <cudf/io/parquet.hpp>
#include <cudf/table/table.hpp>

#include <rmm/resource_ref.hpp>

#include <cuda/stream>

#include <cucascade/data/common.hpp>
#include <cucascade/data/representation_converter.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace sirius::io {
class ioctx;
class sirius_datasource;
}  // namespace sirius::io

namespace sirius::op::scan {
class parquet_ingestible_table_info;
}  // namespace sirius::op::scan

namespace sirius::scan_manager {

/// The cover @p entry gives a parquet scan reading @p materialized (primary indices, in the scan's
/// materialized order), or nullopt when it cannot serve it partly: no chunk sources, other files,
/// a virtual or hive-partition column, nothing (or everything) pinned, or pinned columns below
/// @p min_share of the scan's decoded row width.
[[nodiscard]] std::optional<partial_cover> partial_cover_for(
  pinned_entry const& entry,
  op::scan::parquet_ingestible_table_info const& info,
  std::span<std::size_t const> materialized,
  double min_share);

/// One partly served scan: its pin, its cover, and the files its disk columns are read from. Shared
/// by every chunk the scan serves; opens each file and resolves its footer once.
class partial_pin_scan {
 public:
  partial_pin_scan(std::shared_ptr<pinned_entry const> entry,
                   partial_cover cover,
                   std::shared_ptr<io::ioctx> io_ctx,
                   op::scan::scan_contract_id contract);

  /// Chunk @p chunk's disk columns: its recorded row groups decoded in recorded order, unfiltered,
  /// columns in @ref partial_cover::disk_columns order.
  [[nodiscard]] std::unique_ptr<cudf::table> read_disk_columns(
    std::size_t chunk, ::cuda::stream_ref stream, rmm::device_async_resource_ref mr) const;

  /// Rows chunk @p chunk holds.
  [[nodiscard]] std::size_t chunk_rows(std::size_t chunk) const;

  /// Decoded bytes of chunk @p chunk's disk columns, estimated from the cover's row width.
  [[nodiscard]] std::size_t estimated_disk_bytes(std::size_t chunk) const;

  [[nodiscard]] partial_cover const& cover() const noexcept { return _cover; }

 private:
  struct opened_file {
    std::shared_ptr<io::sirius_datasource> datasource;
    std::shared_ptr<cudf::io::parquet::FileMetaData const> metadata;
  };
  [[nodiscard]] opened_file const& open(std::string const& path) const;

  std::shared_ptr<pinned_entry const> _entry;
  partial_cover _cover;
  std::shared_ptr<io::ioctx> _io_ctx;
  op::scan::scan_contract_id _contract;
  mutable std::mutex _files_mutex;
  mutable std::unordered_map<std::string, opened_file> _files;
};

/// One served chunk: the pin's columns of it and the scan that reads the rest. Converts to a
/// gpu_table_representation holding every column the scan materializes, in its order.
class partial_pin_chunk_representation : public cucascade::idata_representation {
 public:
  partial_pin_chunk_representation(std::unique_ptr<cucascade::idata_representation> pinned,
                                   std::shared_ptr<partial_pin_scan const> scan,
                                   std::size_t chunk);

  [[nodiscard]] std::size_t get_size_in_bytes() const override;
  [[nodiscard]] std::size_t get_uncompressed_data_size_in_bytes() const override;
  [[nodiscard]] std::unique_ptr<cucascade::idata_representation> clone(
    ::cuda::stream_ref stream) const override;

  [[nodiscard]] cucascade::idata_representation const& pinned() const noexcept { return *_pinned; }
  [[nodiscard]] partial_pin_scan const& scan() const noexcept { return *_scan; }
  [[nodiscard]] std::size_t chunk() const noexcept { return _chunk; }

 private:
  std::unique_ptr<cucascade::idata_representation> _pinned;
  std::shared_ptr<partial_pin_scan const> _scan;
  std::size_t _chunk;
};

/// Chunks served partly since the process started, across all scans.
[[nodiscard]] std::uint64_t partial_pin_chunks_served() noexcept;

/// Register the partial_pin_chunk_representation -> gpu_table_representation converter.
void register_partial_pin_converters(cucascade::representation_converter_registry& registry);

/// Serves @p plan's surviving chunks as partial_pin_chunk_representation batches.
/// @p normalization_targets are the scan's carriers in output order, as for a cached scan.
[[nodiscard]] std::unique_ptr<databatch_provider> make_partial_pin_provider(
  std::shared_ptr<partial_pin_scan const> scan,
  std::shared_ptr<pinned_entry const> entry,
  cached_scan_plan plan,
  std::vector<cudf::data_type> normalization_targets);

}  // namespace sirius::scan_manager
