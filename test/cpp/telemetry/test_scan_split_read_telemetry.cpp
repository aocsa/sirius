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

#include "catch.hpp"
#include "sirius_config.hpp"
#include "telemetry/scan_telemetry.hpp"
#include "telemetry/telemetry_context.hpp"

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

using namespace sirius;
using namespace sirius::telemetry;

namespace {

std::string uuid_str(const uuid::UUID& id) { return std::string(uuid::to_string(id)); }

std::vector<std::string> read_all_telemetry_lines(const std::filesystem::path& dir)
{
  std::vector<std::string> lines;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
    if (!entry.is_regular_file()) { continue; }
    std::ifstream in(entry.path());
    std::string line;
    while (std::getline(in, line)) {
      if (!line.empty()) { lines.push_back(line); }
    }
  }
  return lines;
}

std::size_t count_lines_with_all(const std::vector<std::string>& lines,
                                 const std::vector<std::string>& needles)
{
  std::size_t count = 0;
  for (const auto& line : lines) {
    bool all = true;
    for (const auto& needle : needles) {
      if (line.find(needle) == std::string::npos) {
        all = false;
        break;
      }
    }
    if (all) { ++count; }
  }
  return count;
}

}  // namespace

TEST_CASE("scan split reads are Operator statistics on the scan's pipeline",
          "[telemetry][scan_split_read]")
{
  const auto out_dir = std::filesystem::temp_directory_path() /
                       ("sirius_scan_split_read_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(out_dir);

  telemetry_config config;
  config.enable_quent     = true;
  config.output_directory = out_dir.string();
  config.engine_name      = "test-engine";

  const auto pipeline = uuid::now_v7();
  {
    auto context = telemetry_context::create(config);
    emit_scan_split_read(batch_telemetry_info{context.get(), pipeline},
                         scan_split_read{
                           .device_id              = 0,
                           .operator_id            = 3,
                           .sources                = "/data/a.parquet[0,1];/data/b.parquet[2]",
                           .source_files           = 2,
                           .compressed_bytes       = 123456,
                           .estimated_output_bytes = 1000,
                           .rows                   = 10,
                           .output_bytes           = 800,
                           .materialize_ns         = 5000,
                           .finish_ns              = 700,
                         });
    // A null context (no engine, e.g. operator unit tests) is a no-op, not a crash.
    emit_scan_split_read(batch_telemetry_info{nullptr, pipeline}, scan_split_read{});
  }  // the context drops and flushes the ndjson files

  const auto lines = read_all_telemetry_lines(out_dir);
  REQUIRE(!lines.empty());
  REQUIRE(count_lines_with_all(lines,
                               {uuid_str(pipeline),
                                "\"scan_split_read\"",
                                "/data/a.parquet[0,1];/data/b.parquet[2]",
                                "compressed_bytes",
                                "123456",
                                "materialize_ns"}) == 1);

  std::filesystem::remove_all(out_dir);
}
