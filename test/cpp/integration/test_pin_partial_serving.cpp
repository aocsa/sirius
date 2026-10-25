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

// Partial pin serving: with scan_manager.pin_partial_serving on, a scan whose pin holds only some
// of its columns reads those from the pin and the rest of the same rows from disk. Every answer
// must equal DuckDB's own on the CPU, and only scans the pin covers partly may take the path.

#include "scan_manager/partial_pin_serving.hpp"
#include "sirius_context.hpp"
#include "utils/transparent_execution_test_utils.hpp"

#include <catch.hpp>
#include <duckdb.hpp>
#include <unistd.h>
#include <utils/sirius_test_env.hpp>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int kFiles                = 3;
constexpr std::int64_t kRowsPerFile = 25'000;

// k and v get pinned; w and s stay on disk. Row groups of about 10k rows, so chunks hold several.
void generate_parquet(fs::path const& dir)
{
  setenv("SIRIUS_DISABLE", "1", 1);
  {
    duckdb::DuckDB gen_db(nullptr);
    duckdb::Connection gen(gen_db);
    for (int i = 0; i < kFiles; ++i) {
      auto const lo = i * kRowsPerFile;
      auto r        = gen.Query(
        "COPY (SELECT range AS k, range * 7 AS v, (range * 13) % 1000 AS w, "
               "'s' || (range % 17)::VARCHAR AS s FROM range(" +
        std::to_string(lo) + ", " + std::to_string(lo + kRowsPerFile) + ")) TO '" +
        (dir / ("part_" + std::to_string(i) + ".parquet")).string() +
        "' (FORMAT PARQUET, ROW_GROUP_SIZE 10000);");
      REQUIRE(r);
      REQUIRE_FALSE(r->HasError());
    }
  }
  unsetenv("SIRIUS_DISABLE");
}

void write_config(fs::path const& yaml_path, bool partial)
{
  std::ofstream f(yaml_path);
  f << "sirius:\n"
       "  topology:\n"
       "    num_gpus: 1\n"
       "  memory:\n"
       "    gpu:\n"
       "      usage_limit_fraction: 0.4\n"
       "      reservation_limit_fraction: 1.0\n"
       "    host:\n"
       "      capacity_bytes: 32000000000\n"
       "      initial_number_pools: 10\n"
       "      pool_size: 512\n"
       "      block_size: 1048576\n"
       "  executor:\n"
       "    pipeline:\n"
       "      num_threads: 4\n"
       "    task_creator:\n"
       "      num_threads: 2\n"
       "    downgrade:\n"
       "      num_threads: 1\n"
       "      monitor_period: 10ms\n"
       "    scan_manager:\n"
       "      pin_partial_serving: "
    << (partial ? "true" : "false")
    << "\n"
       "  operator_params:\n"
       "    enable_compressed_materialization: true\n"
       "    scan_task_batch_size: 300000\n"
       "    max_sort_partition_bytes: 0\n"
       "    hash_partition_bytes: 100000000\n"
       "    concat_batch_bytes: 100000000\n"
       "    max_build_hash_table_bytes: 90000000\n";
}

void pause_shared_envs()
{
  if (sirius::test::g_shared_env && sirius::test::g_shared_env->is_active()) {
    sirius::test::g_shared_env->pause();
  }
  if (sirius::test::g_integration_env && sirius::test::g_integration_env->is_active()) {
    sirius::test::g_integration_env->pause();
  }
  if (sirius::test::g_integration_env_2gpu && sirius::test::g_integration_env_2gpu->is_active()) {
    sirius::test::g_integration_env_2gpu->pause();
  }
}

std::string run(duckdb::Connection& con, std::string const& sql)
{
  auto r = con.Query(sql);
  if (r->HasError()) { UNSCOPED_INFO(sql << " -> " << r->GetError()); }
  REQUIRE_FALSE(r->HasError());
  return r->ToString();
}

/// @p sql on the GPU and on DuckDB's CPU path; returns the GPU answer after checking they agree.
std::string run_and_compare(duckdb::Connection& con, std::string const& sql)
{
  auto const gpu = run(con, sql);
  run(con, "SET gpu_execution = false;");
  auto const cpu = run(con, sql);
  run(con, "SET gpu_execution = true;");
  INFO(sql);
  REQUIRE(gpu == cpu);
  return gpu;
}

}  // namespace

// No [integration]/[shared_context] tag: this test builds its own context (see
// test_pin_table_merge_columns.cpp).
TEST_CASE("pin_table - a pin serves the columns it holds, disk the rest of the same rows",
          "[pin_table][partial_pin][scan_manager]")
{
  pause_shared_envs();
  auto const tier = GENERATE(std::string{"host"}, std::string{"gpu"});
  CAPTURE(tier);

  auto tmp =
    fs::temp_directory_path() / ("sirius-pin-partial-" + tier + "-" + std::to_string(::getpid()));
  std::error_code ec;
  fs::remove_all(tmp, ec);
  fs::create_directories(tmp);
  generate_parquet(tmp);
  auto const t   = "read_parquet('" + (tmp / "*.parquet").string() + "')";
  auto yaml_path = tmp / "pin_partial.yaml";
  write_config(yaml_path, true);

  {
    sirius::test::shared_test_env local_env(yaml_path);
    auto con = local_env.make_connection();
    run(con, "SET enable_duckdb_fallback = false;");
    run(con,
        "CALL pin_table('" + (tmp / "*.parquet").string() + "', tier='" + tier +
          "', name='kv', cols=['k', 'v']);");

    auto served = [] { return sirius::scan_manager::partial_pin_chunks_served(); };

    // A scan the pin covers fully takes the ordinary cached path.
    auto before = served();
    run_and_compare(con, "SELECT sum(v), count(*) FROM " + t + ";");
    REQUIRE(served() == before);

    // Filter on a disk column: every chunk joins pinned k, v with disk w.
    before = served();
    run_and_compare(con, "SELECT count(*), sum(v), sum(w) FROM " + t + " WHERE w < 500;");
    REQUIRE(served() > before);

    // Filter on a pinned column, which the pin's zone maps may use to skip whole chunks.
    run_and_compare(con, "SELECT count(*), sum(w) FROM " + t + " WHERE k BETWEEN 1000 AND 40000;");

    // Row by row: any misalignment between the halves pairs k with another row's w or s. (No
    // integer-to-VARCHAR cast here: that fails on the GPU whether or not anything is pinned.)
    REQUIRE(run_and_compare(
              con, "SELECT count(*) FROM " + t + " WHERE w <> (k * 13) % 1000 OR v <> k * 7;")
              .find("\n0\n") != std::string::npos);
    run_and_compare(con, "SELECT count(*) FROM (SELECT k % 17, s FROM " + t + " GROUP BY ALL);");
    run_and_compare(
      con, "SELECT s, count(*), sum(v), min(k), max(w) FROM " + t + " GROUP BY s ORDER BY s;");
    run_and_compare(con, "SELECT k, v, w, s FROM " + t + " WHERE k % 997 = 3 ORDER BY k;");
  }
  fs::remove_all(tmp, ec);
}

TEST_CASE("pin_table - with partial serving off, a partly covered scan reads disk only",
          "[pin_table][partial_pin][scan_manager]")
{
  pause_shared_envs();
  auto tmp = fs::temp_directory_path() / ("sirius-pin-partial-off-" + std::to_string(::getpid()));
  std::error_code ec;
  fs::remove_all(tmp, ec);
  fs::create_directories(tmp);
  generate_parquet(tmp);
  auto const t   = "read_parquet('" + (tmp / "*.parquet").string() + "')";
  auto yaml_path = tmp / "pin_partial_off.yaml";
  write_config(yaml_path, false);
  {
    sirius::test::shared_test_env local_env(yaml_path);
    auto con = local_env.make_connection();
    run(con, "SET enable_duckdb_fallback = false;");
    run(con,
        "CALL pin_table('" + (tmp / "*.parquet").string() +
          "', tier='host', name='kv', cols=['k', 'v']);");
    auto const before = sirius::scan_manager::partial_pin_chunks_served();
    run_and_compare(con, "SELECT count(*), sum(v), sum(w) FROM " + t + " WHERE w < 500;");
    REQUIRE(sirius::scan_manager::partial_pin_chunks_served() == before);
  }
  fs::remove_all(tmp, ec);
}
