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

// Simpatico packing of exchanged batches: what EXCHANGE_SINK packs, EXCHANGE_SOURCE unpacks
// unchanged.
#include "catch.hpp"
#include "compression/shuffle_compression.hpp"
#include "exchange/exchange_test_utils.hpp"

#include <cudf/column/column_factories.hpp>

using namespace sirius::test::exchange_utils;
using sirius::compression::compress_for_shuffle;
using sirius::compression::decompress_from_shuffle;
using sirius::compression::is_shuffle_blob_schema;
using sirius::compression::shuffle_compression_stats;

TEST_CASE("a packed batch unpacks to the same table", "[exchange][shuffle_compression]")
{
  auto stream = utils::default_stream();
  auto mr     = utils::get_resource_ref(*env().gpu);
  // INT64 keys and nullable INT32s bitpack; DECIMAL128 and strings are stored as they are.
  auto const table = sample_table(200'000);
  shuffle_compression_stats stats;
  auto packed = compress_for_shuffle(table->view(), stream, mr, stats);
  REQUIRE(packed != nullptr);
  CHECK(is_shuffle_blob_schema(packed->view()));
  CHECK(stats.packed_bytes < stats.raw_bytes);
  auto const unpacked = decompress_from_shuffle(packed->view(), stream, mr);
  CHECK(digest(unpacked->view()) == digest(table->view()));
}

TEST_CASE("a batch with nothing to bitpack is sent as it is", "[exchange][shuffle_compression]")
{
  auto stream = utils::default_stream();
  auto mr     = utils::get_resource_ref(*env().gpu);
  auto const table = sample_table(1'000);
  // Only the DECIMAL128 and string columns: no column bitpacks.
  auto const view = table->view().select({2, 3});
  shuffle_compression_stats stats;
  CHECK(compress_for_shuffle(view, stream, mr, stats) == nullptr);
}
