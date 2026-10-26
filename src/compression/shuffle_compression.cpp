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

#include "compression/shuffle_compression.hpp"

#include "sirius/exception.hpp"

#include <cudf/column/column.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/traits.hpp>
#include <rmm/device_buffer.hpp>

#include <api/compressed_table_io.hpp>
#include <api/simpatico_codegen.hpp>

#include <cuda_runtime_api.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace sirius::compression {
namespace {

constexpr std::uint64_t blob_magic   = 0x4655485350504d53ull;  // "SMPPSHUF"
constexpr std::size_t prefix_bytes   = 3 * sizeof(std::uint64_t);
constexpr std::size_t payload_align  = 256;
constexpr std::string_view bitpack   = "input -> bitpack -> chunk_min, chunk_count, chunk_bits, packed\n";
constexpr std::string_view identity  = "input -> identity\n";

bool bitpackable(cudf::data_type type)
{
  switch (type.id()) {
    case cudf::type_id::INT32:
    case cudf::type_id::INT64:
    case cudf::type_id::TIMESTAMP_DAYS:
    case cudf::type_id::DECIMAL64: return true;
    default: return false;
  }
}

/// Device bytes @p column occupies: its data, null mask and children.
std::size_t column_bytes(cudf::column_view const& column, ::cuda::stream_ref stream)
{
  std::size_t bytes =
    column.nullable() ? cudf::bitmask_allocation_size_bytes(column.size()) : std::size_t{0};
  if (column.type().id() == cudf::type_id::STRING) {
    cudf::strings_column_view strings{column};
    return bytes + static_cast<std::size_t>(strings.chars_size(stream)) +
           (column.size() + 1) * cudf::size_of(strings.offsets().type());
  }
  if (cudf::is_fixed_width(column.type())) {
    bytes += static_cast<std::size_t>(column.size()) * cudf::size_of(column.type());
  }
  for (auto it = column.child_begin(); it != column.child_end(); ++it) {
    bytes += column_bytes(*it, stream);
  }
  return bytes;
}

std::size_t align_up(std::size_t n, std::size_t a) { return (n + a - 1) / a * a; }

}  // namespace

shuffle_compression shuffle_compression_mode()
{
  static shuffle_compression const mode = [] {
    char const* value = std::getenv("SIRIUS_EXCHANGE_COMPRESSION");
    std::string_view const v{value != nullptr ? value : ""};
    if (v.empty() || v == "off") { return shuffle_compression::off; }
    if (v == "staged") { return shuffle_compression::staged; }
    if (v == "all") { return shuffle_compression::all; }
    throw sirius::invalid_input_exception(
      "SIRIUS_EXCHANGE_COMPRESSION must be off, staged or all, not '{}'", v);
  }();
  return mode;
}

bool is_shuffle_blob_schema(cudf::table_view const& table)
{
  return table.num_columns() == 1 && table.column(0).type().id() == cudf::type_id::UINT64;
}

std::unique_ptr<cudf::table> compress_for_shuffle(cudf::table_view table,
                                                  ::cuda::stream_ref stream,
                                                  rmm::device_async_resource_ref mr,
                                                  shuffle_compression_stats& stats)
{
  stats = {};
  for (auto const& column : table) {
    stats.raw_bytes += column_bytes(column, stream);
  }
  bool any = false;
  std::string plan;
  for (cudf::size_type c = 0; c < table.num_columns(); ++c) {
    bool const packs = bitpackable(table.column(c).type());
    any |= packs;
    if (c > 0) { plan += "---\n"; }
    plan += packs ? bitpack : identity;
  }
  if (!any || table.num_rows() == 0) { return nullptr; }

  auto compressed = simpatico::compress_with_plan(table, plan, stream, mr);
  std::vector<std::uint8_t> header;
  std::vector<simpatico::payload_buffer_ref> buffers;
  std::uint64_t payload_bytes = 0;
  if (auto const error = simpatico::build_compressed_table_header(
        compressed, header, buffers, payload_bytes, stream);
      !error.empty()) {
    throw sirius::internal_exception("shuffle compression: {}", error);
  }
  std::size_t const payload_at = align_up(prefix_bytes + header.size(), payload_align);
  std::size_t const total      = align_up(payload_at + payload_bytes, sizeof(std::uint64_t));
  stats.packed_bytes           = total;
  auto const rows              = total / sizeof(std::uint64_t);
  if (static_cast<double>(total) > 0.9 * static_cast<double>(stats.raw_bytes) ||
      rows > static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max())) {
    stream.wait();
    return nullptr;
  }

  rmm::device_buffer blob(total, stream, mr);
  auto* base = static_cast<std::byte*>(blob.data());
  std::vector<std::uint8_t> prefix(prefix_bytes + header.size());
  std::uint64_t const fields[] = {blob_magic, header.size(), payload_bytes};
  std::memcpy(prefix.data(), fields, prefix_bytes);
  std::memcpy(prefix.data() + prefix_bytes, header.data(), header.size());
  CUDF_CUDA_TRY(
    cudaMemcpyAsync(base, prefix.data(), prefix.size(), cudaMemcpyHostToDevice, stream.get()));
  for (auto const& b : buffers) {
    if (b.size_bytes == 0) { continue; }
    CUDF_CUDA_TRY(cudaMemcpyAsync(base + payload_at + b.offset,
                                  b.device_ptr,
                                  b.size_bytes,
                                  cudaMemcpyDeviceToDevice,
                                  stream.get()));
  }
  // The payload buffers belong to `compressed`, and `prefix` is host memory: both must outlive
  // the copies.
  stream.wait();
  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::UINT64},
                                                   static_cast<cudf::size_type>(rows),
                                                   std::move(blob),
                                                   rmm::device_buffer{},
                                                   0));
  return std::make_unique<cudf::table>(std::move(columns));
}

std::unique_ptr<cudf::table> decompress_from_shuffle(cudf::table_view blob,
                                                     ::cuda::stream_ref stream,
                                                     rmm::device_async_resource_ref mr)
{
  if (!is_shuffle_blob_schema(blob)) {
    throw sirius::internal_exception("shuffle compression: not a packed batch");
  }
  auto const* base  = reinterpret_cast<std::byte const*>(blob.column(0).data<std::uint64_t>());
  auto const bytes  = static_cast<std::size_t>(blob.num_rows()) * sizeof(std::uint64_t);
  std::uint64_t fields[3] = {};
  if (bytes < prefix_bytes) { throw sirius::internal_exception("shuffle compression: short batch"); }
  CUDF_CUDA_TRY(
    cudaMemcpyAsync(fields, base, prefix_bytes, cudaMemcpyDeviceToHost, stream.get()));
  stream.wait();
  if (fields[0] != blob_magic || prefix_bytes + fields[1] > bytes) {
    throw sirius::internal_exception("shuffle compression: a batch without its header");
  }
  std::vector<std::uint8_t> header(fields[1]);
  CUDF_CUDA_TRY(cudaMemcpyAsync(
    header.data(), base + prefix_bytes, header.size(), cudaMemcpyDeviceToHost, stream.get()));
  stream.wait();
  std::size_t const payload_at = align_up(prefix_bytes + header.size(), payload_align);
  if (payload_at + fields[2] > bytes) {
    throw sirius::internal_exception("shuffle compression: a batch shorter than its payload");
  }
  std::string error;
  auto compressed = simpatico::read_compressed_table_from_memory(
    header,
    [&](std::uint64_t offset, std::size_t size, void* dst, ::cuda::stream_ref s) {
      CUDF_CUDA_TRY(cudaMemcpyAsync(
        dst, base + payload_at + offset, size, cudaMemcpyDeviceToDevice, s.get()));
    },
    stream,
    mr,
    &error);
  if (!error.empty()) { throw sirius::internal_exception("shuffle compression: {}", error); }
  auto out = simpatico::decompress(compressed, stream, mr);
  stream.wait();
  return out;
}

}  // namespace sirius::compression
