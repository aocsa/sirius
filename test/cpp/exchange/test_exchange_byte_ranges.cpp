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
#include "planner/distributed_plan.hpp"
#include "sirius/exception.hpp"

#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

using sirius::planner::byte_ranges_for_rank;
using sirius::planner::file_byte_range;
using sirius::planner::file_share_for_rank;

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

namespace {

/// Local files of the given sizes in a fresh directory, removed with it.
struct sized_files {
  std::filesystem::path dir;
  std::vector<std::string> paths;

  explicit sized_files(std::vector<std::uint64_t> const& sizes)
    : dir(std::filesystem::temp_directory_path() /
          ("sirius_file_share_" + std::to_string(::getpid())))
  {
    std::filesystem::create_directories(dir);
    for (std::size_t i = 0; i < sizes.size(); ++i) {
      paths.push_back((dir / ("part" + std::to_string(i) + ".parquet")).string());
      std::ofstream(paths.back(), std::ios::binary) << std::string(sizes[i], 'x');
    }
  }
  ~sized_files() { std::filesystem::remove_all(dir); }
};

using ranges = std::vector<std::vector<std::pair<std::uint64_t, std::uint64_t>>>;

}  // namespace

TEST_CASE("file_share_for_rank reads every file whole on one rank", "[exchange][byte_range]")
{
  // No stat either: the files need not exist.
  auto const share = file_share_for_rank({"/nonexistent/a", "/nonexistent/b"}, 0, 1);
  CHECK(share.files == std::vector<std::string>{"/nonexistent/a", "/nonexistent/b"});
  CHECK(share.ranges.empty());
}

TEST_CASE("file_share_for_rank splits local files by byte range", "[exchange][byte_range]")
{
  sized_files const files({600, 200, 100});
  auto const rank0 = file_share_for_rank(files.paths, 0, 2);
  CHECK(rank0.files == std::vector<std::string>{files.paths[0]});
  CHECK(rank0.ranges == ranges{{{0, 450}}});
  // A file wholly inside a rank's span still carries its range: the share is then not the
  // whole-file read a replicated pin and its scans use.
  auto const rank1 = file_share_for_rank(files.paths, 1, 2);
  CHECK(rank1.files == files.paths);
  CHECK(rank1.ranges == ranges{{{450, 150}}, {{0, 200}}, {{0, 100}}});
}

TEST_CASE("file_share_for_rank stripes remote files whole", "[exchange][byte_range]")
{
  std::vector<std::string> const files{"s3://b/0", "s3://b/1", "s3://b/2", "s3://b/3", "s3://b/4"};
  auto const rank0 = file_share_for_rank(files, 0, 2);
  CHECK(rank0.files == std::vector<std::string>{"s3://b/0", "s3://b/2", "s3://b/4"});
  CHECK(rank0.ranges.empty());
  auto const rank1 = file_share_for_rank(files, 1, 2);
  CHECK(rank1.files == std::vector<std::string>{"s3://b/1", "s3://b/3"});
  CHECK_THROWS_AS(file_share_for_rank({"s3://b/0"}, 0, 2), sirius::not_implemented_exception);
}
