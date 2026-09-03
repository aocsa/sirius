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

#include "telemetry/data_batch_probe.hpp"  // batch_telemetry_info

#include <cstdint>
#include <string>

namespace sirius::telemetry {

/// One storage-backed scan split materialized by the GPU scan operator: the read+decode
/// (`gpu_ingestible::materialize_table`, which brackets the datasource I/O and the cudf decode
/// together) plus the post-decode filter/project/normalize that produced the emitted batch.
///
/// Reported as Operator `statistics` on the scan's pipeline (Quent Operator id == Sirius
/// pipeline uuid) with `event = "scan_split_read"`, and stamped with the executor thread so it
/// joins the Task `Computing(GPU_SCAN)` span it ran inside. Resident/cached splits read nothing
/// and are not reported.
struct scan_split_read {
  int32_t device_id{};
  uint64_t operator_id{};
  /// `path[rg,rg,...];path[...]` over the split's source objects.
  std::string sources;
  uint64_t source_files{};
  /// Compressed bytes the read was expected to fetch (0 when the format does not account).
  uint64_t compressed_bytes{};
  uint64_t estimated_output_bytes{};
  uint64_t rows{};
  uint64_t output_bytes{};
  /// Wall time of materialize_table (I/O + decode) and of everything after it, in ns.
  uint64_t materialize_ns{};
  uint64_t finish_ns{};
};

/// No-op when `info.context` is null. Never throws.
void emit_scan_split_read(const batch_telemetry_info& info, const scan_split_read& read) noexcept;

}  // namespace sirius::telemetry
