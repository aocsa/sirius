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

#include "scan_manager/partial_pin_serving.hpp"

#include "compression/compressed_representation.hpp"
#include "data/data_batch_utils.hpp"
#include "data/sirius_converter_registry.hpp"
#include "io/io_context.hpp"
#include "io/sirius_datasource.hpp"
#include "op/scan/parquet_gpu_ingestible.hpp"
#include "op/scan/parquet_materialize.hpp"
#include "op/scan/parquet_metadata.hpp"

#include <rmm/cuda_device.hpp>

#include <cucascade/cudf/gpu_data_representation.hpp>
#include <cucascade/cudf/host_data_representation.hpp>
#include <cucascade/data/data_batch.hpp>
#include <cucascade/memory/memory_space.hpp>
#include <duckdb/common/types.hpp>

#include <algorithm>
#include <atomic>
#include <stdexcept>
#include <utility>

namespace sirius::scan_manager {

std::optional<partial_cover> partial_cover_for(pinned_entry const& entry,
                                               op::scan::parquet_ingestible_table_info const& info,
                                               std::span<std::size_t const> materialized,
                                               double min_share)
{
  if (entry.chunk_sources.empty() || entry.mvcc || materialized.empty()) { return std::nullopt; }
  if (info.has_requested_user_virtual_columns() || !info.partition_indices.empty()) {
    return std::nullopt;
  }
  if (!entry.cache_info.matches_parquet_files(info.resolved_file_paths,
                                              info.resolved_file_ranges)) {
    return std::nullopt;
  }

  partial_cover cover;
  std::size_t pinned_width = 0;
  std::size_t total_width  = 0;
  for (auto const primary : materialized) {
    // Virtual columns (row ids, file names) sit past the file's columns and are never pinned.
    if (primary >= info.names.size() || primary >= info.bound_types.size()) { return std::nullopt; }
    auto const width = duckdb::GetTypeIdSize(info.bound_types[primary].InternalType());
    total_width += width;
    auto const& ids = entry.cache_info.column_ids;
    auto const it   = std::ranges::find_if(
      ids, [primary](duckdb::ColumnIndex const& id) { return id.GetPrimaryIndex() == primary; });
    if (it != ids.end()) {
      cover.pinned_slots.push_back(static_cast<std::size_t>(it - ids.begin()));
      cover.from_pin.push_back(true);
      pinned_width += width;
    } else {
      cover.disk_columns.push_back(info.names[primary]);
      cover.from_pin.push_back(false);
      cover.disk_bytes_per_row += width;
    }
  }
  // All pinned is a full cover, served by the ordinary cached path.
  if (cover.pinned_slots.empty() || cover.disk_columns.empty()) { return std::nullopt; }
  cover.pinned_share = static_cast<double>(pinned_width) / static_cast<double>(total_width);
  if (cover.pinned_share < min_share) { return std::nullopt; }
  return cover;
}

partial_pin_scan::partial_pin_scan(std::shared_ptr<pinned_entry const> entry,
                                   partial_cover cover,
                                   std::shared_ptr<io::ioctx> io_ctx,
                                   op::scan::scan_contract_id contract)
  : _entry(std::move(entry)),
    _cover(std::move(cover)),
    _io_ctx(std::move(io_ctx)),
    _contract(contract)
{
  if (!_entry || !_io_ctx) {
    throw std::invalid_argument("[partial_pin_scan] needs a pinned entry and an io context");
  }
}

partial_pin_scan::opened_file const& partial_pin_scan::open(std::string const& path) const
{
  std::lock_guard lock{_files_mutex};
  if (auto it = _files.find(path); it != _files.end()) { return it->second; }
  std::shared_ptr<io::sirius_datasource> datasource = _io_ctx->open_datasource(path);
  if (!datasource) { throw std::runtime_error("[partial_pin_scan] no io backend serves " + path); }
  // The pin parsed this footer, so the io context's metadata store usually has it.
  auto const metadata = op::scan::resolve_parquet_metadata(
    *datasource, _contract, path, cudf::io::parquet_reader_options::builder().build());
  return _files.emplace(path, opened_file{std::move(datasource), metadata->file_metadata()})
    .first->second;
}

std::unique_ptr<cudf::table> partial_pin_scan::read_disk_columns(
  std::size_t chunk, ::cuda::stream_ref stream, rmm::device_async_resource_ref mr) const
{
  auto const& source = _entry->chunk_sources.at(chunk);
  std::vector<op::scan::parquet_source> sources;
  sources.reserve(source.files.size());
  for (auto const& file : source.files) {
    auto const& opened = open(file.path);
    sources.push_back({opened.datasource, opened.metadata, file.row_groups});
  }
  // No filter: the rows must line up with the pinned chunk's, which holds every row.
  auto options = cudf::io::parquet_reader_options::builder().build();
  options.set_column_names(_cover.disk_columns);
  return op::scan::materialize_parquet(sources, options, {}, stream, mr);
}

std::size_t partial_pin_scan::chunk_rows(std::size_t chunk) const
{
  return _entry->chunk_sources.at(chunk).num_rows;
}

std::size_t partial_pin_scan::estimated_disk_bytes(std::size_t chunk) const
{
  return chunk_rows(chunk) * _cover.disk_bytes_per_row;
}

partial_pin_chunk_representation::partial_pin_chunk_representation(
  std::unique_ptr<cucascade::idata_representation> pinned,
  std::shared_ptr<partial_pin_scan const> scan,
  std::size_t chunk)
  : idata_representation(pinned->get_memory_space()),
    _pinned(std::move(pinned)),
    _scan(std::move(scan)),
    _chunk(chunk)
{
}

std::size_t partial_pin_chunk_representation::get_size_in_bytes() const
{
  return _pinned->get_size_in_bytes() + _scan->estimated_disk_bytes(_chunk);
}

std::size_t partial_pin_chunk_representation::get_uncompressed_data_size_in_bytes() const
{
  return _pinned->get_uncompressed_data_size_in_bytes() + _scan->estimated_disk_bytes(_chunk);
}

std::unique_ptr<cucascade::idata_representation> partial_pin_chunk_representation::clone(
  ::cuda::stream_ref stream) const
{
  return std::make_unique<partial_pin_chunk_representation>(_pinned->clone(stream), _scan, _chunk);
}

namespace {

std::atomic<std::uint64_t> g_chunks_served{0};

std::unique_ptr<cucascade::idata_representation> partial_pin_chunk_to_gpu(
  cucascade::idata_representation const& source,
  cucascade::memory::memory_space const* target,
  ::cuda::stream_ref stream,
  [[maybe_unused]] cucascade::memory::reservation* reservation)
{
  auto const& rep = source.cast<partial_pin_chunk_representation>();
  if (target == nullptr || target->get_tier() != cucascade::memory::Tier::GPU) {
    throw std::invalid_argument("[partial_pin] a chunk converts only into GPU memory");
  }
  auto& space = const_cast<cucascade::memory::memory_space&>(*target);
  rmm::cuda_set_device_raii device_guard{rmm::cuda_device_id{space.get_device_id()}};
  auto mr = space.get_default_allocator();

  std::unique_ptr<cudf::table> pinned;
  if (auto const* gpu = dynamic_cast<cucascade::gpu_table_representation const*>(&rep.pinned())) {
    // Uncompressed GPU pins share their columns with every query; this chunk gets its own copy.
    pinned = std::make_unique<cudf::table>(gpu->get_table_view(), stream, mr);
  } else {
    pinned = converter_registry::get()
               .convert<cucascade::gpu_table_representation>(rep.pinned(), target, stream)
               ->release_table(stream);
  }
  auto disk = rep.scan().read_disk_columns(rep.chunk(), stream, mr);

  auto const rows = rep.scan().chunk_rows(rep.chunk());
  if (static_cast<std::size_t>(pinned->num_rows()) != rows ||
      static_cast<std::size_t>(disk->num_rows()) != rows) {
    throw std::runtime_error("[partial_pin] chunk " + std::to_string(rep.chunk()) + " holds " +
                             std::to_string(rows) + " rows, but its pinned columns have " +
                             std::to_string(pinned->num_rows()) + " and its disk columns " +
                             std::to_string(disk->num_rows()));
  }

  auto const& from_pin = rep.scan().cover().from_pin;
  auto pinned_columns  = pinned->release();
  auto disk_columns    = disk->release();
  if (pinned_columns.size() + disk_columns.size() != from_pin.size()) {
    throw std::runtime_error("[partial_pin] decoded column counts do not match the scan's cover");
  }
  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.reserve(from_pin.size());
  std::size_t p = 0;
  std::size_t d = 0;
  for (bool const pin : from_pin) {
    columns.push_back(std::move(pin ? pinned_columns[p++] : disk_columns[d++]));
  }
  g_chunks_served.fetch_add(1, std::memory_order_relaxed);
  return std::make_unique<cucascade::gpu_table_representation>(
    std::make_unique<cudf::table>(std::move(columns)), space, stream);
}

/// Serves the survivor chunks of a partly covered scan, each as one
/// partial_pin_chunk_representation.
struct partial_pin_provider : public databatch_provider {
  partial_pin_provider(std::shared_ptr<partial_pin_scan const> scan,
                       std::shared_ptr<pinned_entry const> entry,
                       cached_scan_plan plan,
                       std::vector<cudf::data_type> normalization_targets)
    : _scan(std::move(scan)),
      _entry(std::move(entry)),
      _plan(std::move(plan)),
      _normalization_targets(std::move(normalization_targets))
  {
  }

  batch get_next_batch() override
  {
    auto const cursor = _index.fetch_add(1);
    if (cursor >= _plan.survivor_chunk_indices.size()) { return {}; }
    auto const index = _plan.survivor_chunk_indices[cursor];
    auto pinned      = pinned_columns(index);
    if (!pinned) { return {}; }
    batch out;
    out.data = cucascade::data_batch::make(
      get_next_batch_id(),
      std::make_unique<partial_pin_chunk_representation>(std::move(pinned), _scan, index));
    out.needs_carrier_conversion = pinned_carriers_differ(index);
    return out;
  }

 private:
  /// Chunk @p index's pinned columns of this scan, projected as the cached provider projects them,
  /// without its decode-time row selection: both halves must keep every row.
  std::unique_ptr<cucascade::idata_representation> pinned_columns(std::size_t index) const
  {
    auto const& slots = _scan->cover().pinned_slots;
    auto const& entry = *_entry;
    if (entry.tier == cucascade::memory::Tier::HOST) {
      if (index >= entry.host_chunks.size() || !entry.host_chunks[index]) { return nullptr; }
      auto const& chunk = entry.host_chunks[index];
      if (auto* compressed = dynamic_cast<sirius::compressed_host_representation*>(chunk.get())) {
        return compressed->select_columns(slots);
      }
      return chunk->cast<cucascade::host_data_representation>().slice(slots);
    }
    if (!entry.device_chunks.empty()) {
      if (index >= entry.device_chunks.size()) { return nullptr; }
      auto const& chunk = entry.device_chunks[index];
      if (chunk.compressed) { return chunk.compressed->select_columns(slots); }
      std::vector<std::shared_ptr<cudf::column>> columns;
      for (auto const slot : slots) {
        if (slot >= chunk.columns.size() || !chunk.columns[slot]) { return nullptr; }
        columns.push_back(chunk.columns[slot]);
      }
      auto* space = chunk.memory_space ? chunk.memory_space : entry.memory_space;
      return gpu_view(std::move(columns), *space);
    }
    if (index >= entry.chunk_memory_spaces.size()) { return nullptr; }
    std::vector<std::shared_ptr<cudf::column>> columns;
    for (auto const slot : slots) {
      auto const& chunks = entry.data_batches_by_column.at(entry.cache_info.names.at(slot));
      if (index >= chunks.size() || !chunks[index]) { return nullptr; }
      columns.push_back(chunks[index]);
    }
    return gpu_view(std::move(columns), *entry.chunk_memory_spaces[index]);
  }

  static std::unique_ptr<cucascade::idata_representation> gpu_view(
    std::vector<std::shared_ptr<cudf::column>> columns, cucascade::memory::memory_space& space)
  {
    std::vector<cudf::column_view> views;
    std::size_t bytes = 0;
    for (auto const& column : columns) {
      views.push_back(column->view());
      bytes += column->alloc_size();
    }
    return std::make_unique<cucascade::gpu_table_representation>(
      cudf::table_view{views},
      std::move(columns),
      bytes,
      space,
      ::cuda::stream_ref{cudaStream_t{}});
  }

  /// Whether scan normalization will cast a pinned column of chunk @p index: its stored carrier
  /// differs from the carrier the scan plans for it. Disk columns decode at that carrier already.
  bool pinned_carriers_differ(std::size_t index) const
  {
    auto const& matrix = _entry->column_storage;
    if (index >= matrix.size()) { return false; }
    auto const& cover = _scan->cover();
    std::size_t slot  = 0;
    for (std::size_t position = 0; position < cover.from_pin.size(); ++position) {
      if (!cover.from_pin[position]) { continue; }
      auto const entry_position = cover.pinned_slots[slot++];
      if (position < _normalization_targets.size() && entry_position < matrix[index].size() &&
          matrix[index][entry_position].carrier != _normalization_targets[position]) {
        return true;
      }
    }
    return false;
  }

  std::shared_ptr<partial_pin_scan const> _scan;
  std::shared_ptr<pinned_entry const> _entry;
  cached_scan_plan _plan;
  std::vector<cudf::data_type> _normalization_targets;
  std::atomic<std::size_t> _index{0};
};

}  // namespace

std::uint64_t partial_pin_chunks_served() noexcept
{
  return g_chunks_served.load(std::memory_order_relaxed);
}

void register_partial_pin_converters(cucascade::representation_converter_registry& registry)
{
  if (!registry
         .has_converter<partial_pin_chunk_representation, cucascade::gpu_table_representation>()) {
    registry
      .register_converter<partial_pin_chunk_representation, cucascade::gpu_table_representation>(
        partial_pin_chunk_to_gpu);
  }
}

std::unique_ptr<databatch_provider> make_partial_pin_provider(
  std::shared_ptr<partial_pin_scan const> scan,
  std::shared_ptr<pinned_entry const> entry,
  cached_scan_plan plan,
  std::vector<cudf::data_type> normalization_targets)
{
  return std::make_unique<partial_pin_provider>(
    std::move(scan), std::move(entry), std::move(plan), std::move(normalization_targets));
}

}  // namespace sirius::scan_manager
