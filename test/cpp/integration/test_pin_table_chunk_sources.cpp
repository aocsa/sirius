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

// A parquet pin records, per chunk, the row groups the chunk decoded (pinned_entry::chunk_sources).
// Reading those row groups back from disk must give exactly the chunk's rows, in order: that is
// what lets a scan read a pin's missing columns from disk and line them up with the pinned ones.

#include "scan_manager/sirius_scan_manager.hpp"
#include "sirius_context.hpp"
#include "utils/transparent_execution_test_utils.hpp"

#include <cudf/column/column.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/parquet_metadata.hpp>
#include <cudf/table/table.hpp>

#include <cuda_runtime.h>

#include <catch.hpp>
#include <duckdb.hpp>
#include <unistd.h>
#include <utils/sirius_test_env.hpp>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int kFiles                = 3;
constexpr std::int64_t kRowsPerFile = 25'000;

// Three files of 25k rows each, in row groups of about 10k rows, so a chunk can hold several row
// groups and span files. Written by a Sirius-disabled DuckDB, as in the other pin tests.
void generate_parquet(fs::path const& dir)
{
  setenv("SIRIUS_DISABLE", "1", 1);
  {
    duckdb::DuckDB gen_db(nullptr);
    duckdb::Connection gen(gen_db);
    for (int i = 0; i < kFiles; ++i) {
      auto const lo = i * kRowsPerFile;
      auto r        = gen.Query("COPY (SELECT range AS k, range * 7 AS v FROM range(" +
                         std::to_string(lo) + ", " + std::to_string(lo + kRowsPerFile) + ")) TO '" +
                         (dir / ("part_" + std::to_string(i) + ".parquet")).string() +
                         "' (FORMAT PARQUET, ROW_GROUP_SIZE 10000);");
      REQUIRE(r);
      REQUIRE_FALSE(r->HasError());
    }
  }
  unsetenv("SIRIUS_DISABLE");
}

// Narrowing off, so pinned columns keep the INT64 a disk read decodes. A 300 kB scan batch is
// about 19k rows of (k, v): a few row groups per chunk.
void write_config(fs::path const& yaml_path)
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
       "  operator_params:\n"
       "    enable_compressed_materialization: false\n"
       "    scan_task_batch_size: 300000\n"
       "    max_sort_partition_bytes: 0\n"
       "    hash_partition_bytes: 100000000\n"
       "    concat_batch_bytes: 100000000\n"
       "    max_build_hash_table_bytes: 90000000\n";
}

std::vector<std::int64_t> to_host(cudf::column_view const& col)
{
  REQUIRE(col.type() == cudf::data_type{cudf::type_id::INT64});
  REQUIRE(col.null_count() == 0);
  std::vector<std::int64_t> out(static_cast<std::size_t>(col.size()));
  REQUIRE(cudaMemcpy(out.data(),
                     col.data<std::int64_t>(),
                     out.size() * sizeof(std::int64_t),
                     cudaMemcpyDeviceToHost) == cudaSuccess);
  return out;
}

/// The chunk's row groups read from disk, file by file in recorded order: column `name`.
std::vector<std::int64_t> read_from_disk(sirius::pinned_chunk_source const& source,
                                         std::string const& name)
{
  std::vector<std::int64_t> out;
  for (auto const& file : source.files) {
    auto opts = cudf::io::parquet_reader_options::builder(cudf::io::source_info{file.path})
                  .columns({name})
                  .row_groups({file.row_groups})
                  .build();
    auto read = cudf::io::read_parquet(opts);
    auto part = to_host(read.tbl->view().column(0));
    out.insert(out.end(), part.begin(), part.end());
  }
  return out;
}

sirius::scan_manager::pinned_entry const& find_entry(duckdb::SiriusContext& ctx,
                                                     std::string_view wanted)
{
  sirius::scan_manager::pinned_entry const* ptr = nullptr;
  ctx.get_scan_manager().visit_pinned_entries([&](std::string_view name, auto const& entry) {
    if (name != wanted) { return true; }  // the visit stops at the first false
    ptr = &entry;
    return false;
  });
  REQUIRE(ptr != nullptr);
  return *ptr;
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

}  // namespace

// No [integration]/[shared_context] tag: this test builds its own context (see
// test_pin_table_merge_columns.cpp).
TEST_CASE("pin_table - each chunk records the parquet row groups it holds",
          "[pin_table][chunk_sources][scan_manager]")
{
  pause_shared_envs();

  auto tmp = fs::temp_directory_path() / ("sirius-pin-sources-" + std::to_string(::getpid()));
  std::error_code ec;
  fs::remove_all(tmp, ec);
  fs::create_directories(tmp);
  generate_parquet(tmp);
  auto const glob = (tmp / "*.parquet").string();
  auto yaml_path  = tmp / "pin_sources.yaml";
  write_config(yaml_path);

  {
    sirius::test::shared_test_env local_env(yaml_path);
    auto con = local_env.make_connection();
    auto fb  = con.Query("SET enable_duckdb_fallback = false;");
    REQUIRE_FALSE(fb->HasError());
    auto ctx = con.context->registered_state->Get<duckdb::SiriusContext>("sirius_state");
    REQUIRE(ctx != nullptr);

    auto pin = [&](std::string const& tier) {
      auto r = con.Query("CALL pin_table('" + glob + "', tier='" + tier + "', name='src_" + tier +
                         "', cols=['k', 'v']);");
      if (r->HasError()) { UNSCOPED_INFO("pin_table " << tier << ": " << r->GetError()); }
      REQUIRE_FALSE(r->HasError());
    };
    pin("gpu");
    auto const& gpu = find_entry(*ctx, "src_gpu");

    // One source per chunk, every file one of the entry's, rows summing to the entry's.
    auto const& k_chunks = gpu.data_batches_by_column.at("k");
    auto const& v_chunks = gpu.data_batches_by_column.at("v");
    REQUIRE(gpu.chunk_sources.size() == k_chunks.size());
    REQUIRE(gpu.chunk_sources.size() > 1);
    REQUIRE_FALSE(sirius::scan_manager::chunk_sources_problem(gpu, gpu.chunk_sources));
    REQUIRE(gpu.num_rows == static_cast<std::size_t>(kFiles * kRowsPerFile));

    // The fixture is meant to exercise chunks of several row groups.
    bool multi_row_group = false;
    for (auto const& source : gpu.chunk_sources) {
      std::size_t groups = 0;
      for (auto const& file : source.files) {
        groups += file.row_groups.size();
      }
      multi_row_group = multi_row_group || groups > 1;
    }
    REQUIRE(multi_row_group);

    // Every row group of every file belongs to exactly one chunk.
    std::set<std::pair<std::string, cudf::size_type>> seen;
    std::size_t recorded = 0;
    for (auto const& source : gpu.chunk_sources) {
      for (auto const& file : source.files) {
        for (auto rg : file.row_groups) {
          seen.emplace(file.path, rg);
          ++recorded;
        }
      }
    }
    std::size_t on_disk = 0;
    for (auto const& path : gpu.cache_info.resolved_file_paths) {
      on_disk += static_cast<std::size_t>(
        cudf::io::read_parquet_metadata(cudf::io::source_info{path}).num_rowgroups());
    }
    REQUIRE(recorded == seen.size());
    REQUIRE(seen.size() == on_disk);

    // The round trip: a chunk's recorded row groups, read from disk, are the chunk's rows.
    for (std::size_t c = 0; c < gpu.chunk_sources.size(); ++c) {
      INFO("chunk " << c);
      auto const& source  = gpu.chunk_sources[c];
      auto const k_pinned = to_host(k_chunks[c]->view());
      REQUIRE(source.num_rows == k_pinned.size());
      REQUIRE(read_from_disk(source, "k") == k_pinned);
      REQUIRE(read_from_disk(source, "v") == to_host(v_chunks[c]->view()));
    }

    // Serving is unchanged.
    auto sum = con.Query("SELECT sum(v) FROM read_parquet('" + glob + "');");
    REQUIRE_FALSE(sum->HasError());
    std::int64_t const n = kFiles * kRowsPerFile;
    REQUIRE(sum->Collection().GetValue(0, 0).ToString() == std::to_string(7 * (n * (n - 1) / 2)));

    // Sources that do not describe the entry are reported, not trusted.
    auto sources = gpu.chunk_sources;
    sources.pop_back();
    REQUIRE(sirius::scan_manager::chunk_sources_problem(gpu, sources));
    sources = gpu.chunk_sources;
    sources.front().num_rows += 1;
    REQUIRE(sirius::scan_manager::chunk_sources_problem(gpu, sources));
    sources                            = gpu.chunk_sources;
    sources.front().files.front().path = (tmp / "elsewhere.parquet").string();
    REQUIRE(sirius::scan_manager::chunk_sources_problem(gpu, sources));
    sources = gpu.chunk_sources;
    sources.front().files.clear();
    REQUIRE(sirius::scan_manager::chunk_sources_problem(gpu, sources));

    // The batching depends on the pinned columns, not the tier: a host pin of the same columns
    // names the same rows.
    pin("host");
    auto const& host = find_entry(*ctx, "src_host");
    REQUIRE(host.host_chunks.size() == host.chunk_sources.size());
    REQUIRE(host.chunk_sources == find_entry(*ctx, "src_gpu").chunk_sources);
  }
  fs::remove_all(tmp, ec);
}
