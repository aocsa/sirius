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

// How a distributed scan splits a table's files across ranks by byte range.

#include "catch.hpp"
#include "op/scan/parquet_scan_types.hpp"
#include "planner/distributed_plan.hpp"
#include "sirius_config.hpp"
#include "utils/parquet_fixture_utils.hpp"
#include "utils/table_filter_test_utils.hpp"

#include <duckdb/common/multi_file/multi_file_reader.hpp>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

using sirius::planner::byte_ranges_for_rank;
using sirius::planner::file_byte_range;

namespace {

/// Every byte of every file is owned by exactly one rank, and each rank's ranges come in file
/// order, one per file.
void require_exact_tiling(std::vector<std::uint64_t> const& files, std::size_t world)
{
  std::vector<std::vector<int>> owners(files.size());
  for (std::size_t f = 0; f < files.size(); ++f) {
    owners[f].assign(files[f], 0);
  }
  for (std::size_t rank = 0; rank < world; ++rank) {
    auto const mine   = byte_ranges_for_rank(files, rank, world);
    std::size_t prior = 0;
    for (std::size_t i = 0; i < mine.size(); ++i) {
      auto const& r = mine[i];
      REQUIRE(r.length > 0);
      REQUIRE(r.start + r.length <= files[r.file]);
      if (i > 0) { REQUIRE(r.file > prior); }
      prior = r.file;
      for (auto b = r.start; b < r.start + r.length; ++b) {
        ++owners[r.file][b];
      }
    }
  }
  for (auto const& file : owners) {
    for (auto const count : file) {
      REQUIRE(count == 1);
    }
  }
}

}  // namespace

TEST_CASE("byte_ranges_for_rank tiles every file exactly once", "[exchange][byte_range]")
{
  require_exact_tiling({100}, 1);
  require_exact_tiling({100}, 2);
  require_exact_tiling({101}, 3);
  require_exact_tiling({10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, 2);
  require_exact_tiling({7, 300, 1, 55, 1000, 2}, 4);
  require_exact_tiling({5, 5}, 8);
}

TEST_CASE("byte_ranges_for_rank splits one file between two ranks", "[exchange][byte_range]")
{
  std::vector<std::uint64_t> const files{1000};
  CHECK(byte_ranges_for_rank(files, 0, 2) == std::vector<file_byte_range>{{0, 0, 500}});
  CHECK(byte_ranges_for_rank(files, 1, 2) == std::vector<file_byte_range>{{0, 500, 500}});
}

TEST_CASE("byte_ranges_for_rank balances unevenly sized files by bytes", "[exchange][byte_range]")
{
  // Whole-file striping would give rank 0 files 0 and 2 (700 bytes) and rank 1 only file 1.
  std::vector<std::uint64_t> const files{600, 200, 100};
  CHECK(byte_ranges_for_rank(files, 0, 2) == std::vector<file_byte_range>{{0, 0, 450}});
  CHECK(byte_ranges_for_rank(files, 1, 2) ==
        std::vector<file_byte_range>{{0, 450, 150}, {1, 0, 200}, {2, 0, 100}});
}

TEST_CASE("assign_files_to_rank selects files by file_index before splitting them",
          "[exchange][byte_range]")
{
  // Three files of 1000 bytes: unselected, rank 1 scans half of file 1 and file 2.
  sirius::test::scratch_dir scratch{"assign_files_by_index"};
  std::vector<std::string> files;
  for (int f = 0; f < 3; ++f) {
    files.push_back((scratch.path() / ("part" + std::to_string(f) + ".parquet")).string());
    std::ofstream(files.back()) << std::string(1000, 'x');
  }
  auto const file_index = duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX;
  auto const share      = [&](std::size_t rank, std::uint64_t wanted) {
    auto owned = std::make_unique<sirius::op::scan::parquet_ingestible_table_info>();
    auto& info = *owned;
    info.column_ids.emplace_back(0ULL);
    info.column_ids.emplace_back(file_index);
    info.names          = {"k"};
    info.returned_types = {sirius::logical_type::make(sirius::type_id::BIGINT)};
    info.virtual_columns.push_back(
      {file_index, "file_index", sirius::logical_type::make(sirius::type_id::UBIGINT), {}});
    info.table_filters = duckdb::make_uniq<duckdb::TableFilterSet>();
    info.table_filters->PushFilter(
      duckdb::ProjectionIndex(1),
      sirius::test::constant_filter(duckdb::ExpressionType::COMPARE_EQUAL,
                                    duckdb::Value::UBIGINT(wanted)));
    sirius::operator_params params;
    params.exchange_peers = {"127.0.0.1:1", "127.0.0.1:2"};
    params.exchange_rank  = static_cast<std::uint32_t>(rank);
    sirius::planner::assign_files_to_rank(info, files, params);
    return owned;
  };

  // Each rank scans half of the selected file, under the table's index.
  for (std::size_t rank = 0; rank < 2; ++rank) {
    auto const mine = share(rank, 2);
    CHECK(mine->resolved_file_paths == std::vector<std::string>{files[2]});
    CHECK(mine->resolved_file_indices == std::vector<std::size_t>{2});
    CHECK(mine->resolved_file_ranges.size() == 1);
  }
  // None selected: rank 1 keeps its share of all three, which the scan finds unselected too.
  auto const none = share(1, 7);
  CHECK(none->resolved_file_paths == std::vector<std::string>{files[1], files[2]});
  CHECK(none->resolved_file_indices == std::vector<std::size_t>{1, 2});
}
