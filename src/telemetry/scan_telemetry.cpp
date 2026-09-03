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

#include "telemetry/scan_telemetry.hpp"

#include "telemetry-bridge/gen/dynamic_attributes.rs.h"
#include "telemetry-bridge/gen/operator.rs.h"
#include "telemetry/telemetry_context.hpp"

#include <string>
#include <utility>

namespace sirius::telemetry {

void emit_scan_split_read(const batch_telemetry_info& info, const scan_split_read& read) noexcept
{
  if (info.context == nullptr) { return; }
  try {
    quent::DynamicAttributes attributes;
    auto put_string = [&attributes](const char* key, std::string value) {
      attributes.string_attrs.push_back(quent::StringAttr{.key = key, .value = std::move(value)});
    };
    auto put_i64 = [&attributes](const char* key, uint64_t value) {
      attributes.i64_attrs.push_back(
        quent::I64Attr{.key = key, .value = static_cast<int64_t>(value)});
    };

    put_string("event", "scan_split_read");
    put_string("sources", read.sources);
    // The thread the split ran on: joins this event to the Task Computing(GPU_SCAN) span that
    // was active on that thread when it was emitted.
    put_string("executor_thread",
               executor_thread_telemetry_handle.has_value()
                 ? std::string(uuid::to_string(executor_thread_telemetry_handle->handle->uuid()))
                 : std::string{});
    put_i64("device_id", static_cast<uint64_t>(read.device_id));
    put_i64("operator_id", read.operator_id);
    put_i64("source_files", read.source_files);
    put_i64("compressed_bytes", read.compressed_bytes);
    put_i64("estimated_output_bytes", read.estimated_output_bytes);
    put_i64("rows", read.rows);
    put_i64("output_bytes", read.output_bytes);
    put_i64("materialize_ns", read.materialize_ns);
    put_i64("finish_ns", read.finish_ns);

    auto observer = quent::operator_::create_observer(info.context->context());
    observer->statistics(info.producer_pipeline_uuid,
                         quent::operator_::Statistics{.custom_attributes = std::move(attributes)});
  } catch (...) {
  }
}

}  // namespace sirius::telemetry
