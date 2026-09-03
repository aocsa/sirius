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
#include "data/data_batch_utils.hpp"
#include "memory/sirius_memory_reservation_manager.hpp"
#include "sirius_config.hpp"
#include "telemetry/batch_telemetry.hpp"
#include "telemetry/telemetry_context.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/table/table.hpp>
#include <cudf/utilities/default_stream.hpp>

#include <cucascade/memory/memory_space.hpp>
#include <cucascade/memory/reservation_manager_configurator.hpp>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
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

std::unique_ptr<memory::sirius_memory_reservation_manager> make_memory_manager()
{
  cucascade::memory::reservation_manager_configurator builder;
  builder.set_number_of_gpus(1)
    .set_gpu_usage_limit(2ull << 27)
    .set_reservation_fraction_per_gpu(0.75)
    .set_per_numa_region_capacity(4ull << 27)
    .use_gpu_id_as_host_id()
    .set_reservation_fraction_per_numa_region(0.75);
  return std::make_unique<memory::sirius_memory_reservation_manager>(builder.build());
}

}  // namespace

TEST_CASE("stream-hop batch placements are adopted by the claiming task",
          "[telemetry][batch_stream_hop]")
{
  const auto out_dir = std::filesystem::temp_directory_path() /
                       ("sirius_batch_stream_hop_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(out_dir);

  telemetry_config config;
  config.enable_quent     = true;
  config.output_directory = out_dir.string();
  config.engine_name      = "test-engine";

  auto manager   = make_memory_manager();
  auto& registry = batch_telemetry_registry::instance();
  // The registry is process-global; make sure no earlier test left it installed.
  registry.uninstall();

  uint64_t relayed_id = 0, pushed_id = 0;
  std::string task_id;
  {
    auto context = telemetry_context::create(config, manager.get(), {0});
    registry.install(context, *manager);

    auto* gpu_space = manager->get_memory_space(cucascade::memory::Tier::GPU, 0);
    REQUIRE(gpu_space != nullptr);
    auto stream = cudf::get_default_stream();
    // A wire batch has no local producing pipeline: push_packed builds it with a nil producer.
    auto make_wire_batch = [&] {
      std::vector<std::unique_ptr<cudf::column>> columns;
      columns.push_back(cudf::make_numeric_column(cudf::data_type{cudf::type_id::INT32},
                                                  8,
                                                  cudf::mask_state::UNALLOCATED,
                                                  stream,
                                                  gpu_space->get_default_allocator()));
      return make_data_batch(std::make_unique<cudf::table>(std::move(columns)),
                             *gpu_space,
                             stream,
                             batch_telemetry_info{context.get(), uuid::new_nil()});
    };

    // relay_from: the hop is registered before any consumer pipeline exists, then the
    // receiver's task claims, computes on, and consumes the batch.
    auto relayed = make_wire_batch();
    relayed_id   = relayed->get_batch_id();
    registry.on_stream_hop(relayed, batch_origin::stream_relayed);
    const auto pipeline = uuid::now_v7();
    const auto task     = uuid::now_v7();
    task_id             = uuid_str(task);
    registry.on_packaged(relayed, pipeline, task);
    registry.on_processing(relayed, task);
    registry.on_consumed(relayed_id, task);

    // push_packed: same hop with its own origin; left queued to be drained at uninstall.
    auto pushed = make_wire_batch();
    pushed_id   = pushed->get_batch_id();
    registry.on_stream_hop(pushed, batch_origin::stream_pushed);

    registry.uninstall();
  }  // batches and context drop and flush the ndjson files

  const auto lines       = read_all_telemetry_lines(out_dir);
  const auto relayed_key = "\"batch_id\":" + std::to_string(relayed_id);
  const auto pushed_key  = "\"batch_id\":" + std::to_string(pushed_id);
  const auto nil_uuid    = uuid_str(uuid::new_nil());
  REQUIRE(!lines.empty());

  // Registered once, by the hop: the claim adopted that placement instead of lazily
  // registering a second `reschedule_intermediate` one.
  REQUIRE(count_lines_with_all(lines, {"\"BatchRegistered\"", relayed_key}) == 1);
  REQUIRE(count_lines_with_all(lines, {"\"BatchRegistered\"", relayed_key, "\"stream_relayed\""}) ==
          1);
  REQUIRE(count_lines_with_all(lines, {"\"BatchPackaged\"", task_id}) == 1);
  REQUIRE(count_lines_with_all(lines, {"\"BatchProcessing\"", task_id}) == 1);
  REQUIRE(count_lines_with_all(lines, {"\"BatchConsumed\"", "\"processed\""}) == 1);

  // The pushed batch is its own placement with the push_packed origin, and its DataBatch names
  // no producing pipeline (the same shape push_packed emits for an unpacked wire batch).
  REQUIRE(count_lines_with_all(lines, {"\"BatchRegistered\"", pushed_key, "\"stream_pushed\""}) ==
          1);
  REQUIRE(
    count_lines_with_all(
      lines, {"\"Constructed\"", "\"data_batch_id\":" + std::to_string(pushed_id), nil_uuid}) == 1);

  std::filesystem::remove_all(out_dir);
}
