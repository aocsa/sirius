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
#include "exec/exchange_staging_arena.hpp"
#include "memory/sirius_memory_reservation_manager.hpp"
#include "sirius_config.hpp"
#include "telemetry/memory_context.hpp"
#include "telemetry/staging_arena_telemetry.hpp"
#include "telemetry/telemetry_context.hpp"

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

bool any_line_with_all(const std::vector<std::string>& lines,
                       const std::vector<std::string>& needles)
{
  return count_lines_with_all(lines, needles) > 0;
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

TEST_CASE("staging arena leases are DataBatches on an exchange-staging-arena Memory",
          "[telemetry][staging_arena_telemetry]")
{
  const auto out_dir = std::filesystem::temp_directory_path() /
                       ("sirius_staging_arena_telemetry_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(out_dir);

  telemetry_config config;
  config.enable_quent     = true;
  config.output_directory = out_dir.string();
  config.engine_name      = "test-engine";

  auto manager = make_memory_manager();
  std::string engine_id, arena_id, gpu_id;
  {
    auto context    = telemetry_context::create(config, manager.get(), {0});
    engine_id       = uuid_str(context->engine_id());
    auto* gpu_space = manager->get_memory_space(cucascade::memory::Tier::GPU, 0);
    REQUIRE(gpu_space != nullptr);
    auto gpu_handle = context->get_memory_context()->get_memory_handle(gpu_space->get_id());
    REQUIRE(gpu_handle.has_value());
    gpu_id = uuid_str(gpu_handle->get().uuid());

    exec::exchange_staging_arena arena(1u << 20);
    auto probe =
      std::make_shared<staging_arena_telemetry>(context, manager.get(), arena.capacity());
    arena_id = uuid_str(probe->memory_id());
    arena.attach_probe(probe);

    // Send side: lease, pack GPU -> arena, done, release.
    const auto a = arena.lease(4096);
    REQUIRE(probe->live_leases() == 1);
    probe->on_pack_started(a, gpu_space->get_id(), 4000);
    probe->on_pack_completed(a);

    // Receive side: lease, unpack arena -> GPU, then released while still in transit (the
    // probe must settle it before destructing, as in_transit -> destructed is not a transition).
    const auto b = arena.lease(100);
    probe->on_unpack_started(b, gpu_space->get_id(), 64);
    arena.release(b);
    arena.release(a);
    REQUIRE(probe->live_leases() == 0);

    // Unknown offsets and spaces are ignored, never thrown.
    probe->on_pack_started(12345, gpu_space->get_id(), 1);
    probe->on_release(12345);
  }  // probe, arena, then the context drop and flush the ndjson files

  const auto lines = read_all_telemetry_lines(out_dir);
  REQUIRE(!lines.empty());

  // The arena is a Memory under the engine, with a channel each way to the GPU memory space.
  REQUIRE(any_line_with_all(lines, {"\"exchange-staging-arena\"", engine_id, arena_id}));
  REQUIRE(any_line_with_all(lines, {"gpu-0->exchange-staging-arena", gpu_id, arena_id}));
  REQUIRE(any_line_with_all(lines, {"exchange-staging-arena->gpu-0", arena_id, gpu_id}));

  // Two leases, each a staging_lease DataBatch: stationary on the arena for its booked
  // length, in transit with the packed byte count, and destructed on release.
  REQUIRE(count_lines_with_all(lines, {"\"staging_lease\""}) == 2);
  REQUIRE(any_line_with_all(lines, {"\"Stationary\"", arena_id, "\"capacity_bytes\":4096"}));
  REQUIRE(any_line_with_all(lines, {"\"Stationary\"", arena_id, "\"capacity_bytes\":256"}));
  REQUIRE(any_line_with_all(lines, {"\"InTransit\"", gpu_id, arena_id, "\"capacity_bytes\":4000"}));
  REQUIRE(any_line_with_all(lines, {"\"InTransit\"", arena_id, gpu_id, "\"capacity_bytes\":64"}));
  REQUIRE(count_lines_with_all(lines, {"\"Destructed\""}) == 2);

  std::filesystem::remove_all(out_dir);
}
