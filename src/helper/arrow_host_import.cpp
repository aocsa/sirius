/*
 * Copyright 2025, Sirius Contributors.
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

#include "helper/arrow_host_import.hpp"

#include "cudf/cudf_utils.hpp"  // sirius::get_cudf_type
#include "log/logging.hpp"
#include "memory/size_arithmetic.hpp"  // sirius::memory::saturating_add
#include "sirius/exception.hpp"

// The Arrow C Data Interface structs (ArrowSchema, ArrowArray). DuckDB's copy is the one every
// other TU of this library sees (sirius_ffi.cpp reads result streams through it), it is a hard
// dependency in every build flavour, and it is layout-identical to Apache Arrow's abi.h. Apache
// Arrow's own header is NOT a dependency of this tree (cudf's vcpkg port brings nanoarrow, the
// pixi default env only has it through pyarrow), so including it here would break the vcpkg
// build and give libsirius two definitions of the same struct.
#include "duckdb/common/arrow/arrow.hpp"

#include <cudf/interop.hpp>
#include <cudf/null_mask.hpp>                  // cudf::bitmask_allocation_size_bytes
#include <cudf/unary.hpp>                      // cudf::cast, cudf::is_supported_cast
#include <cudf/utilities/traits.hpp>           // cudf::is_fixed_point, cudf::size_of
#include <cudf/utilities/type_dispatcher.hpp>  // cudf::type_to_name

#include <rmm/aligned.hpp>  // rmm::align_up, rmm::CUDA_ALLOCATION_ALIGNMENT

#include <cucascade/memory/memory_reservation.hpp>
#include <cucascade/memory/memory_space.hpp>
#include <cucascade/memory/reservation_aware_resource_adaptor.hpp>

#include <algorithm>  // std::max
#include <bit>        // std::popcount
#include <charconv>   // std::from_chars
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace sirius {

namespace {

std::string_view format_of(const ArrowSchema& schema)
{
  return schema.format == nullptr ? std::string_view{} : std::string_view{schema.format};
}

// Shapes cudf would import into something the engine cannot consume, or would import with a
// silently changed meaning. Refused by name so the producer learns which column and why; checked
// before any buffer is read, so a bad batch costs no device memory.
void refuse_unsupported_shape(std::string_view what,
                              std::size_t index,
                              const std::string& name,
                              const ArrowSchema& child,
                              const logical_type& declared)
{
  const auto refuse = [&](const std::string& reason) {
    throw invalid_input_exception("{}: column {} ({}) {}", what, index, name, reason);
  };
  const auto format = format_of(child);

  if (child.dictionary != nullptr) {
    refuse("is dictionary-encoded; the engine consumes plain columns — decode it before pushing");
  }
  if (format == "+L") {
    refuse("is a large_list (64-bit offsets); the engine's list columns use 32-bit offsets");
  }
  if (format == "U") {
    refuse(
      "is large_utf8 (64-bit offsets); the engine's string kernels take 32-bit offsets — send "
      "utf8");
  }
  if (format == "Z") {
    refuse(
      "is large_binary (64-bit offsets); the engine's string kernels take 32-bit offsets — send "
      "binary");
  }
  // Timestamps are "ts<unit>:<timezone>"; an empty timezone is a naive timestamp.
  if (format.size() > 4 && format.substr(0, 2) == "ts" && format[3] == ':') {
    refuse("is a timezone-aware timestamp (" + std::string(format.substr(4)) +
           "); the engine has no timezone carrier — convert to a naive timestamp first");
  }
  // Decimals are "d:<precision>,<scale>[,<bitwidth>]"; a bitwidth of 256 has no cudf carrier.
  if (format.substr(0, 2) == "d:" && format.size() > 4 &&
      format.substr(format.size() - 4) == ",256") {
    refuse("is a decimal256; cudf has no 256-bit decimal carrier");
  }
  if (declared.id() == type_id::HUGEINT || declared.id() == type_id::UHUGEINT) {
    refuse("is declared " + declared.to_string() +
           "; the GPU has no 128-bit integer carrier and the value would be narrowed to 64 bits — "
           "declare a DECIMAL or a 64-bit integer instead");
  }
}

// The cudf type `cudf::from_arrow` yields for the scalar formats the engine's columns arrive in,
// or nullopt for a format this table does not know (nested types, binary, float16, date64,
// durations, the explicit-bitwidth decimals): the check on the imported table is the backstop for
// those. Knowing the type from the format string alone is what lets a plain type mismatch be
// refused before any buffer is copied, as `push_packed` refuses one before its deep copy.
std::optional<cudf::data_type> cudf_type_of_format(std::string_view format)
{
  using cudf::type_id;
  if (format == "b") { return cudf::data_type{type_id::BOOL8}; }
  if (format == "c") { return cudf::data_type{type_id::INT8}; }
  if (format == "C") { return cudf::data_type{type_id::UINT8}; }
  if (format == "s") { return cudf::data_type{type_id::INT16}; }
  if (format == "S") { return cudf::data_type{type_id::UINT16}; }
  if (format == "i") { return cudf::data_type{type_id::INT32}; }
  if (format == "I") { return cudf::data_type{type_id::UINT32}; }
  if (format == "l") { return cudf::data_type{type_id::INT64}; }
  if (format == "L") { return cudf::data_type{type_id::UINT64}; }
  if (format == "f") { return cudf::data_type{type_id::FLOAT32}; }
  if (format == "g") { return cudf::data_type{type_id::FLOAT64}; }
  if (format == "u") { return cudf::data_type{type_id::STRING}; }
  if (format == "tdD") { return cudf::data_type{type_id::TIMESTAMP_DAYS}; }
  // Naive timestamps only ("ts<unit>:"); the timezone-aware form was refused by name above.
  if (format == "tss:") { return cudf::data_type{type_id::TIMESTAMP_SECONDS}; }
  if (format == "tsm:") { return cudf::data_type{type_id::TIMESTAMP_MILLISECONDS}; }
  if (format == "tsu:") { return cudf::data_type{type_id::TIMESTAMP_MICROSECONDS}; }
  if (format == "tsn:") { return cudf::data_type{type_id::TIMESTAMP_NANOSECONDS}; }
  // "d:<precision>,<scale>" is decimal128; a third field is an explicit bitwidth, not mapped here.
  if (format.substr(0, 2) == "d:") {
    const auto comma = format.find(',');
    if (comma == std::string_view::npos || format.find(',', comma + 1) != std::string_view::npos) {
      return std::nullopt;
    }
    const auto digits = format.substr(comma + 1);
    if (digits.empty()) { return std::nullopt; }
    int scale             = 0;
    const auto* const end = digits.data() + digits.size();
    const auto [ptr, ec]  = std::from_chars(digits.data(), end, scale);
    if (ec != std::errc{} || ptr != end) { return std::nullopt; }
    return cudf::data_type{type_id::DECIMAL128, -scale};
  }
  return std::nullopt;
}

// The one disagreement the import tolerates: fixed-point types that differ only in storage width.
// Arrow producers emit decimal128 whatever the precision; the declared precision picks the
// engine's width and the column is narrowed to it after the copy.
bool differs_in_width_only(cudf::data_type actual, cudf::data_type expected)
{
  return cudf::is_fixed_point(actual) && cudf::is_fixed_point(expected) &&
         actual.scale() == expected.scale();
}

[[noreturn]] void throw_type_mismatch(std::string_view what,
                                      std::size_t index,
                                      const std::string& name,
                                      const logical_type& declared,
                                      cudf::data_type expected,
                                      cudf::data_type actual)
{
  // Two fixed-point types that differ in scale would pass the width cast with a silently shifted
  // value, so name the scales: the width alone would send the producer the wrong way.
  std::string scale_note;
  if (cudf::is_fixed_point(actual) && cudf::is_fixed_point(expected) &&
      actual.scale() != expected.scale()) {
    scale_note = "; declared scale " + std::to_string(-expected.scale()) + " but carries scale " +
                 std::to_string(-actual.scale());
  }
  throw invalid_input_exception("{}: column {} ({}) is declared {} ({}) but carries {}{}",
                                what,
                                index,
                                name,
                                declared.to_string(),
                                cudf::type_to_name(expected),
                                cudf::type_to_name(actual),
                                scale_note);
}

// Nulls in bits [begin, end) of an Arrow validity bitmap (LSB first).
std::int64_t count_nulls(const std::uint8_t* validity, std::int64_t begin, std::int64_t end)
{
  std::int64_t valid = 0;
  auto bit           = begin;
  for (; bit < end && (bit % 8) != 0; ++bit) {
    valid += (validity[bit / 8] >> (bit % 8)) & 1;
  }
  for (; bit + 8 <= end; bit += 8) {
    valid += std::popcount(validity[bit / 8]);
  }
  for (; bit < end; ++bit) {
    valid += (validity[bit / 8] >> (bit % 8)) & 1;
  }
  return (end - begin) - valid;
}

// A producer may slice the struct itself (Arrow C++ `StructArray::Slice`) rather than its columns
// (arrow-rs `RecordBatch::slice`), or hand over children longer than the struct; both are legal
// C Data Interface shapes whose rows are the struct's [offset, offset + length). cudf imports each
// child by the child's own offset and length and ignores the struct's, so the window is pushed
// into a shallow copy of each child before its import, the normalization arrow-rs applies on
// export. Refuses a window that runs past a child, naming the column.
void validate_struct_window(const ArrowArray& array,
                            std::string_view what,
                            const std::vector<std::string>& names)
{
  if (array.offset < 0 || array.length < 0) {
    throw invalid_input_exception("{}: the struct array has a negative offset ({}) or length ({})",
                                  what,
                                  array.offset,
                                  array.length);
  }
  for (std::size_t i = 0; i < static_cast<std::size_t>(array.n_children); ++i) {
    const auto& child = *array.children[i];
    if (child.length < array.offset + array.length) {
      throw invalid_input_exception(
        "{}: column {} ({}) has {} rows but the batch spans rows [{}, {}) of its columns",
        what,
        i,
        names[i],
        child.length,
        array.offset,
        array.offset + array.length);
    }
  }
}

// Shallow copy of child `i` restricted to the struct's window: points at the caller's buffers and
// is released by nobody. Only the window's rows are copied by the import. The child's null_count
// describes the whole child; the window's is recounted unless it is known to be 0.
ArrowArray windowed_child(const ArrowArray& array, std::size_t i)
{
  ArrowArray child = *array.children[i];
  if (array.offset == 0 && child.length == array.length) { return child; }
  child.offset += array.offset;
  child.length = array.length;
  const auto* validity =
    child.n_buffers > 0 ? static_cast<const std::uint8_t*>(child.buffers[0]) : nullptr;
  child.null_count = (validity == nullptr || child.null_count == 0)
                       ? 0
                       : count_nulls(validity, child.offset, child.offset + child.length);
  return child;
}

// The checks both entry points run before anything is read: null or released structs, the
// declared name/type agreement, a struct top level, and the column count on both structs.
void validate_struct_batch(const ArrowSchema* schema,
                           const ArrowArray* array,
                           std::string_view what,
                           const std::vector<std::string>& names,
                           const std::vector<logical_type>& types)
{
  if (schema == nullptr || array == nullptr) {
    throw invalid_input_exception("{}: requires non-null ArrowSchema and ArrowArray pointers",
                                  what);
  }
  // The C Data Interface marks a released struct by nulling its `release`; its buffer pointers
  // are dangling from then on, so this has to be a loud error, not a read of freed memory.
  if (schema->release == nullptr || array->release == nullptr) {
    throw invalid_input_exception(
      "{}: the ArrowSchema/ArrowArray were already released (release == NULL)", what);
  }
  if (names.size() != types.size()) {
    throw internal_exception(
      "{}: {} declared names but {} declared types", what, names.size(), types.size());
  }
  if (format_of(*schema) != "+s") {
    throw invalid_input_exception(
      "{}: the top-level Arrow array must be a struct (one record batch), but its format is '{}'",
      what,
      format_of(*schema));
  }
  if (static_cast<std::size_t>(schema->n_children) != types.size() ||
      static_cast<std::size_t>(array->n_children) != types.size()) {
    throw invalid_input_exception(
      "{}: carries {} columns (schema) / {} columns (array) but the stream declares {}",
      what,
      schema->n_children,
      array->n_children,
      types.size());
  }
}

// Allocator granularity: every device buffer the import makes is rounded up to this.
std::size_t aligned_bytes(std::size_t bytes)
{
  return rmm::align_up(bytes, rmm::CUDA_ALLOCATION_ALIGNMENT);
}

std::size_t saturating_add(std::size_t a, std::size_t b) { return memory::saturating_add(a, b); }

// Device bytes of one imported column at `declared`, from the windowed host child.
std::size_t resident_bytes_of(const ArrowArray& child, cudf::data_type declared)
{
  const auto rows   = static_cast<std::size_t>(child.length);
  std::size_t bytes = 0;
  const bool has_validity =
    child.n_buffers > 0 && child.buffers != nullptr && child.buffers[0] != nullptr;
  if (has_validity) {
    bytes = saturating_add(
      bytes,
      aligned_bytes(cudf::bitmask_allocation_size_bytes(static_cast<cudf::size_type>(rows))));
  }
  if (declared.id() == cudf::type_id::STRING) {
    // 32-bit offsets (rows + 1) plus the characters of the window, read off the host offsets.
    bytes = saturating_add(bytes, aligned_bytes((rows + 1) * sizeof(std::int32_t)));
    if (rows > 0 && child.n_buffers > 1 && child.buffers[1] != nullptr) {
      const auto* offsets = static_cast<const std::int32_t*>(child.buffers[1]);
      const auto begin    = offsets[child.offset];
      const auto end      = offsets[child.offset + child.length];
      if (end > begin) {
        bytes = saturating_add(bytes, aligned_bytes(static_cast<std::size_t>(end - begin)));
      }
    }
    return bytes;
  }
  if (cudf::is_nested(declared)) { return bytes; }  // children not walked; see the header
  return saturating_add(bytes, aligned_bytes(rows * cudf::size_of(declared)));
}

// The arriving width of a decimal format string: 128 for `d:p,s` and `d:p,s,128`, the third
// field otherwise; nullopt for anything that is not a decimal.
std::optional<int> decimal_bitwidth_of(std::string_view format)
{
  if (format.substr(0, 2) != "d:") { return std::nullopt; }
  const auto first = format.find(',');
  if (first == std::string_view::npos) { return std::nullopt; }
  const auto second = format.find(',', first + 1);
  if (second == std::string_view::npos) { return 128; }
  int width             = 0;
  const auto digits     = format.substr(second + 1);
  const auto* const end = digits.data() + digits.size();
  const auto [ptr, ec]  = std::from_chars(digits.data(), end, width);
  if (ec != std::errc{} || ptr != end) { return std::nullopt; }
  return width;
}

// The largest allocation alive beside the resident columns while `child` is imported.
std::size_t transient_bytes_of(const ArrowSchema& child_schema,
                               const ArrowArray& child,
                               cudf::data_type declared)
{
  const auto rows   = static_cast<std::size_t>(child.length);
  const auto format = format_of(child_schema);
  if (format == "b") {
    // cudf lands the host bitmap on the device before it expands it to one byte a value.
    return aligned_bytes(cudf::bitmask_allocation_size_bytes(static_cast<cudf::size_type>(rows)));
  }
  if (const auto width = decimal_bitwidth_of(format);
      width && cudf::is_fixed_point(declared) &&
      static_cast<std::size_t>(*width / 8) > cudf::size_of(declared)) {
    // Held at the arriving width until the cast to the declared width lands.
    std::size_t wide = aligned_bytes(rows * static_cast<std::size_t>(*width / 8));
    if (child.n_buffers > 0 && child.buffers != nullptr && child.buffers[0] != nullptr) {
      wide = saturating_add(
        wide,
        aligned_bytes(cudf::bitmask_allocation_size_bytes(static_cast<cudf::size_type>(rows))));
    }
    return wide;
  }
  return 0;
}

}  // namespace

std::unique_ptr<cudf::table> import_arrow_host_table(const ArrowSchema* schema,
                                                     const ArrowArray* array,
                                                     std::string_view what,
                                                     const std::vector<std::string>& names,
                                                     const std::vector<logical_type>& types,
                                                     rmm::cuda_stream_view stream,
                                                     rmm::device_async_resource_ref mr)
{
  validate_struct_batch(schema, array, what, names, types);

  std::vector<cudf::data_type> expected;
  expected.reserve(types.size());
  for (std::size_t i = 0; i < types.size(); ++i) {
    const auto& child = *schema->children[i];
    refuse_unsupported_shape(what, i, names[i], child, types[i]);
    expected.push_back(get_cudf_type(types[i]));
    // A plain type mismatch, refused from the format string before any buffer is copied. Formats
    // the table does not know fall through to the check on the imported column.
    const auto carried = cudf_type_of_format(format_of(child));
    if (carried && *carried != expected[i] && !differs_in_width_only(*carried, expected[i])) {
      throw_type_mismatch(what, i, names[i], types[i], expected[i], *carried);
    }
  }
  validate_struct_window(*array, what, names);

  // Host -> device copy, one column at a time; cudf owns the result, the input is not released.
  // Each column is reconciled to its declared width before the next is imported, so the wide
  // decimal128 a producer emits is alive for one column only, never for the whole batch.
  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.reserve(types.size());
  try {
    for (std::size_t i = 0; i < types.size(); ++i) {
      const ArrowArray window = windowed_child(*array, i);
      auto column             = cudf::from_arrow_column(schema->children[i], &window, stream, mr);
      const auto actual       = column->type();
      if (actual != expected[i]) {
        if (!differs_in_width_only(actual, expected[i]) ||
            !cudf::is_supported_cast(actual, expected[i])) {
          throw_type_mismatch(what, i, names[i], types[i], expected[i], actual);
        }
        // The cast's output is the only other allocation; the wide column is freed when `column`
        // is reassigned, before the next child is touched.
        column = cudf::cast(column->view(), expected[i], stream, mr);
      }
      columns.push_back(std::move(column));
    }
  } catch (...) {
    // The copies may still be reading the producer's buffers (from pinned host memory a
    // cudaMemcpyAsync is truly asynchronous), and the contract lets the producer free them the
    // moment control returns, on the error path too. The device memory is released
    // stream-ordered by the columns themselves.
    stream.synchronize_no_throw();
    throw;
  }
  return std::make_unique<cudf::table>(std::move(columns));
}

std::size_t arrow_import_footprint::peak_bytes() const noexcept
{
  return memory::saturating_add(resident_bytes, transient_bytes);
}

arrow_import_footprint estimate_arrow_import_footprint(const ArrowSchema* schema,
                                                       const ArrowArray* array,
                                                       std::string_view what,
                                                       const std::vector<std::string>& names,
                                                       const std::vector<logical_type>& types)
{
  validate_struct_batch(schema, array, what, names, types);
  validate_struct_window(*array, what, names);

  arrow_import_footprint footprint;
  for (std::size_t i = 0; i < types.size(); ++i) {
    // HUGEINT is refused by the import before any copy; it has no cudf carrier to size here.
    if (types[i].id() == type_id::HUGEINT || types[i].id() == type_id::UHUGEINT) { continue; }
    const auto declared     = get_cudf_type(types[i]);
    const ArrowArray window = windowed_child(*array, i);
    footprint.resident_bytes =
      saturating_add(footprint.resident_bytes, resident_bytes_of(window, declared));
    footprint.transient_bytes = std::max(
      footprint.transient_bytes, transient_bytes_of(*schema->children[i], window, declared));
  }
  return footprint;
}

std::size_t arrow_export_scratch_bytes(const cudf::table_view& table)
{
  std::size_t scratch = 0;
  const auto rows     = static_cast<std::size_t>(table.num_rows());
  const auto mask     = [&](cudf::column_view const& column) {
    return column.nullable() ? aligned_bytes(cudf::bitmask_allocation_size_bytes(column.size()))
                                 : std::size_t{0};
  };
  const auto walk = [&](cudf::column_view const& column, auto& self) -> void {
    const auto type = column.type();
    if (type.id() == cudf::type_id::DECIMAL32 || type.id() == cudf::type_id::DECIMAL64) {
      // Widened to decimal128 on the device before the copy.
      scratch = saturating_add(scratch, saturating_add(aligned_bytes(rows * 16), mask(column)));
    } else if (type.id() == cudf::type_id::BOOL8) {
      // Packed to a bitmap on the device before the copy.
      scratch =
        saturating_add(scratch, aligned_bytes(cudf::bitmask_allocation_size_bytes(column.size())));
    }
    for (cudf::size_type c = 0; c < column.num_children(); ++c) {
      self(column.child(c), self);
    }
  };
  for (auto const& column : table) {
    walk(column, walk);
  }
  return scratch;
}

arrow_transfer_reservation::arrow_transfer_reservation(cucascade::memory::memory_space& space,
                                                       rmm::cuda_stream_view stream,
                                                       std::size_t bytes,
                                                       std::string_view what)
  : stream_(stream), bytes_(bytes)
{
  if (bytes == 0) {
    granted_ = true;
    return;
  }
  auto reservation = space.make_reservation_or_null(bytes);
  if (reservation == nullptr) {
    // Same degrade as the result collector's host reservation: say so, then let the copies
    // compete for headroom as unreserved allocations do.
    SIRIUS_LOG_WARN(
      "{}: GPU reservation of {} bytes failed ({} bytes reserved of a {} byte limit) -- "
      "proceeding without a reservation, the import may OOM",
      what,
      bytes,
      space.get_total_reserved_memory(),
      space.get_max_memory());
    return;
  }
  granted_        = true;
  auto* allocator = reservation->get_memory_resource_of<cucascade::memory::Tier::GPU>();
  // attach_reservation_to_tracker consumes the reservation even when it declines (a state is
  // already attached for this stream/thread), so ask first and hold the reservation unattached
  // in that case rather than lose it.
  if (allocator != nullptr && !allocator->is_stream_tracked(stream) &&
      allocator->attach_reservation_to_tracker(stream, std::move(reservation))) {
    allocator_ = allocator;
    return;
  }
  held_ = std::move(reservation);
}

arrow_transfer_reservation::~arrow_transfer_reservation()
{
  if (allocator_ != nullptr) { allocator_->reset_stream_reservation(stream_); }
  held_.reset();
}

}  // namespace sirius
