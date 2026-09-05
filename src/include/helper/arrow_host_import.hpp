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

#pragma once

#include "helper/logical_type.hpp"

#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/resource_ref.hpp>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

// Arrow C Data Interface structs. Forward-declared only, so this header needs no Arrow header
// and can sit next to any definition of them (DuckDB's `duckdb/common/arrow/arrow.hpp`, Apache
// Arrow's `arrow/c/abi.h`, a vendored copy — all under the shared ARROW_C_DATA_INTERFACE guard,
// all layout-identical). The .cpp uses DuckDB's, the one definition this library already has.
struct ArrowSchema;
struct ArrowArray;

namespace cucascade::memory {
class memory_space;
class reservation;
class reservation_aware_resource_adaptor;
}  // namespace cucascade::memory

namespace sirius {

/**
 * @brief Import one host-memory Arrow struct array (a record batch) into a `cudf::table` and
 * reconcile it against the column types a stream was declared with.
 *
 * The engine reads a stream's columns through the schema it was declared with, so a
 * disagreement between the declaration and the arriving batch must be a loud error at the entry
 * point, not reinterpreted bits downstream — the same rule `Fragment::push_packed` applies with
 * `sirius::get_cudf_type` column by column. On top of that guard this helper:
 *
 * - refuses **by name**, before any buffer is touched, the shapes cudf would import into
 *   something the engine cannot consume or would import with a changed meaning: dictionary
 *   encoding, `large_list` / `large_utf8` / `large_binary` (64-bit offsets — the engine's string
 *   and list kernels take 32-bit offsets), timezone-aware timestamps (no timezone carrier on the
 *   GPU), `decimal256`, and a column declared `HUGEINT`/`UHUGEINT` (cudf has no 128-bit integer;
 *   `get_cudf_type` would narrow it to 64 bits). Only top-level columns are inspected;
 * - refuses a plain type mismatch from the child's format string, before any buffer is copied,
 *   for the scalar formats it knows (integers, floats, bool, utf8, date32, naive timestamps,
 *   decimal128); the check on the imported table is the backstop for every other format;
 * - honours the struct's own `offset`/`length` window — Arrow C++ `StructArray::Slice`, or
 *   children longer than the struct — by pushing it into shallow copies of the children before
 *   the import, since `cudf::from_arrow` reads each child by its own offset and ignores the
 *   struct's. A child shorter than the window is refused naming the column;
 * - picks the decimal storage width from the **declared** precision (DECIMAL32/64/128 with cudf's
 *   negated scale) and narrows an arriving decimal128 to it, since Arrow producers emit
 *   decimal128 whatever the precision. Values that overflow the declared precision are the
 *   producer's schema violation and are truncated by the cast;
 * - relies on cudf for the remaining normalizations the proposal lists: the Arrow bool bitmap
 *   becomes BOOL8, `utf8` becomes STRING with 32-bit offsets, `date32` becomes TIMESTAMP_DAYS.
 *
 * The columns are imported **one at a time** (`cudf::from_arrow_column` per child) and each is
 * reconciled to its declared width before the next child is touched, so the transient device
 * footprint is the table at the declared widths plus one column at its arriving width, never the
 * whole batch at the arriving width and again at the declared one.
 * `estimate_arrow_import_footprint` gives the same numbers ahead of the copy, for a reservation.
 *
 * The buffers are copied to the device on `stream`; the caller synchronizes before it lets the
 * producer release the Arrow structs. On every error path after the copy has started the helper
 * synchronizes `stream` itself before it throws, so the producer's buffers are quiescent whenever
 * control returns. The input is never released by this function.
 *
 * @param schema Arrow schema of the struct array (`+s`), one child per declared column.
 * @param array  Arrow array in host memory whose children are the columns.
 * @param what   Message prefix naming the batch, e.g. `"Arrow batch for stream 3"`.
 * @param names  Declared column names, used only in messages.
 * @param types  Declared column types; the import is reconciled against `get_cudf_type` of each.
 * @param stream CUDA stream for the host-to-device copies and the decimal casts.
 * @param mr     Device memory resource the table is allocated from.
 * @return The imported table, column types equal to `get_cudf_type(types[i])` for every `i`.
 * @throws sirius::invalid_input_exception on null pointers, on structs that were already
 *         released (`release == NULL`), a non-struct top-level array, a column-count mismatch,
 *         a struct window that runs past a child, a refused shape, or a type mismatch (a
 *         fixed-point scale mismatch names both scales); every per-column message names the
 *         column by index and declared name.
 */
std::unique_ptr<cudf::table> import_arrow_host_table(const ArrowSchema* schema,
                                                     const ArrowArray* array,
                                                     std::string_view what,
                                                     const std::vector<std::string>& names,
                                                     const std::vector<logical_type>& types,
                                                     rmm::cuda_stream_view stream,
                                                     rmm::device_async_resource_ref mr);

/**
 * @brief Device bytes `import_arrow_host_table` allocates for a batch, computed from the host
 * structs before any copy.
 *
 * `resident_bytes` is the imported table at the declared widths (data, string offsets and
 * characters, and a null mask for every child that carries a validity buffer), each buffer rounded
 * up to the allocator's 256-byte granularity. `transient_bytes` is the largest extra allocation
 * alive while one column is imported: a decimal128 column that the declared precision narrows is
 * held at 16 bytes a row until its cast lands, and a bool column's bitmap is copied to the device
 * before cudf expands it to BOOL8. Nested columns (STRUCT, LIST) contribute nothing: their buffers
 * are not walked, so the estimate is a floor for them and exact for the scalar set.
 */
struct arrow_import_footprint {
  std::size_t resident_bytes{0};
  std::size_t transient_bytes{0};

  /// Bytes to reserve so the import never allocates beyond what the space accounted for.
  [[nodiscard]] std::size_t peak_bytes() const noexcept;
};

/**
 * @brief Estimate the device footprint of importing `array` at the declared `types`.
 *
 * Runs the same checks as `import_arrow_host_table`, in the same order, before it sizes anything:
 * the structural ones (null or released structs, a non-struct top level, a column-count mismatch,
 * a struct window past a child) and the per-column ones (the by-name refusals, the format string
 * against the declared type), and throws the same errors. It reads only the schema, the array
 * lengths and, for a `utf8` child, the string offsets, never the data; a column declared VARCHAR
 * whose format is not `utf8` is refused rather than read as offsets it does not carry.
 */
arrow_import_footprint estimate_arrow_import_footprint(const ArrowSchema* schema,
                                                       const ArrowArray* array,
                                                       std::string_view what,
                                                       const std::vector<std::string>& names,
                                                       const std::vector<logical_type>& types);

/**
 * @brief Device scratch `cudf::to_arrow_host` allocates while exporting `table`.
 *
 * The D2H copy itself allocates nothing on the device for fixed-width, string, date and timestamp
 * columns; a DECIMAL32/DECIMAL64 column is first widened to decimal128 on the device (16 bytes a
 * row, plus its null mask) and a BOOL8 column is first packed to a bitmap. Nested columns are
 * walked recursively. The sum over all columns is returned, an upper bound on what is alive at
 * once.
 */
std::size_t arrow_export_scratch_bytes(const cudf::table_view& table);

/**
 * @brief Reserve device memory for an Arrow transfer for the lifetime of this object.
 *
 * The result collector's pattern for the bytes an FFI hop is about to allocate: ask the memory
 * space for `bytes` through `make_reservation_or_null`; when the space cannot grant it, log a
 * warning and proceed without one (the allocation then competes for headroom like an unreserved
 * one, never silently more than that). A granted reservation is attached to the space's allocation
 * tracker for `stream` (the calling thread, when the space tracks per thread) so the copies made
 * on it are charged against the reservation rather than counted a second time; when the tracker is
 * already busy (a reservation is attached on this thread) the reservation is held unattached
 * instead, which double-counts only for the lifetime of this object. The destructor detaches and
 * releases: the unused part of the reservation is returned, and what was allocated stays accounted
 * as ordinary pool memory owned by the batch. Create and destroy on the same thread, with `stream`
 * the stream the allocations are made on.
 */
class arrow_transfer_reservation {
 public:
  arrow_transfer_reservation(cucascade::memory::memory_space& space,
                             rmm::cuda_stream_view stream,
                             std::size_t bytes,
                             std::string_view what);
  ~arrow_transfer_reservation();

  arrow_transfer_reservation(const arrow_transfer_reservation&)            = delete;
  arrow_transfer_reservation& operator=(const arrow_transfer_reservation&) = delete;
  arrow_transfer_reservation(arrow_transfer_reservation&&)                 = delete;
  arrow_transfer_reservation& operator=(arrow_transfer_reservation&&)      = delete;

  /// The space granted the reservation (always true for a zero-byte request, which reserves
  /// nothing).
  [[nodiscard]] bool granted() const noexcept { return granted_; }

  /// Allocations on `stream` (this thread) are charged against the reservation.
  [[nodiscard]] bool attached() const noexcept { return allocator_ != nullptr; }

  [[nodiscard]] std::size_t requested_bytes() const noexcept { return bytes_; }

 private:
  cucascade::memory::reservation_aware_resource_adaptor* allocator_{nullptr};
  std::unique_ptr<cucascade::memory::reservation> held_;
  rmm::cuda_stream_view stream_;
  std::size_t bytes_;
  bool granted_{false};
};

}  // namespace sirius
