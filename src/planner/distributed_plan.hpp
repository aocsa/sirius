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

#include "exchange/fabric.hpp"
#include "op/dynamic_filter/dynamic_filter_publish_plan.hpp"
#include "op/sirius_physical_exchange_sink.hpp"
#include "op/sirius_physical_operator.hpp"

#include <cudf/io/parquet_schema.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace cucascade::memory {
class memory_space;
}

namespace sirius::exchange {
class exchange_service;
}

namespace sirius::exec {
class query_lifecycle_registry;
}

namespace duckdb {
class ClientContext;
}

namespace sirius::op {
class sirius_physical_exchange_sink;
class sirius_physical_hash_join;
class sirius_physical_sort_sample;
class sirius_physical_table_scan;
}  // namespace sirius::op

namespace sirius::op::scan {
class parquet_ingestible_table_info;
}

namespace sirius {
struct operator_params;
}

namespace sirius::planner {

struct connector;

using parquet_footer_reader =
  std::function<std::shared_ptr<cudf::io::parquet::FileMetaData const>(std::string const& path)>;

/// What distribute_plan needs to defer a join's probe scan (partition_exchange::defer_scan).
struct deferred_scan_policy {
  parquet_footer_reader footers;  ///< Reads the scan's row counts. Empty turns deferral off.
  std::uint64_t min_bytes = 0;    ///< The smallest scan, over all ranks, that is deferred.
};

/// Which stages distribute_plan keeps on rank 0 instead of on every rank. Zero turns a rule off.
struct stage_policy {
  /// An ORDER BY of at most this many estimated rows is gathered to rank 0 and sorted there.
  std::uint64_t gather_sort_rows = 0;
  /// A hashed join whose two inputs are estimated at this many bytes or fewer in all runs on
  /// rank 0 alone, and so does what is above it, until an operator spreads its rows again.
  std::uint64_t stage_bytes = 0;
  /// A hashed join's build side is counted as it runs: a build of at most this many bytes over
  /// all ranks goes whole to every rank, and the probe rows stay where they are.
  std::uint64_t broadcast_bytes = 0;
};

/// What an external planner tells distribute_plan to do at a join or a grouped merge, which it
/// names by the scanned columns of the site's keys.
struct exchange_directive {
  /// HASH_JOIN or MERGE_GROUP_BY.
  op::SiriusPhysicalOperatorType site;
  /// HASH_JOIN: the two columns of each equality, in either order; for a broadcast, the side to
  /// broadcast first. MERGE_GROUP_BY: the group columns, each as a pair's first.
  std::vector<std::pair<std::string, std::string>> keys;
  /// HASH_JOIN: broadcast (of the build side; the probe side stays where it is) or hash (of both
  /// sides). MERGE_GROUP_BY: hash, or nothing for no exchange below the merge.
  std::optional<op::exchange_spec::mode> routing;
  /// A hashed HASH_JOIN: 1 runs it on rank 0 alone, 0 on every rank.
  exchange::rank_t width = 0;
};

/// One query's directives, in any order. A site takes the one directive that names its key
/// columns, read through projections, filters and joins down to the Parquet scans. A site that
/// no directive names, or that several do, is decided as with no directives; so is every site
/// when there are none.
///
/// A directive moves an exchange and never changes the answer: where it would, it is not
/// followed, and its site is decided as with no directives. That is a broadcast under a join
/// that emits unmatched build rows, a broadcast of the side the plan probes with, and no
/// exchange below a grouped merge unless a hash exchange below it, on columns among the
/// group's, has put each group's rows on one rank.
using exchange_directives = std::vector<exchange_directive>;

/// Directives from text, one per line: `HASH_JOIN a=b[,c=d] hash [width=1]`,
/// `HASH_JOIN a=b[,c=d] broadcast` (a and c on the side to broadcast) or
/// `MERGE_GROUP_BY a[,b] hash|none`. A `#` starts a comment; lines with nothing else are skipped.
/// @throws sirius::invalid_input_exception on any other line.
exchange_directives parse_exchange_directives(std::istream& in);

/// parse_exchange_directives of the file at @p path.
/// @throws sirius::invalid_input_exception also when the file cannot be read.
exchange_directives read_exchange_directives(std::string const& path);

/**
 * @brief Rewrites a plan, after insert_gpu_pipeline_operators, to run on every rank of
 * @p service, each over its own share of the scanned files.
 *
 * Exchanges go where the single-process plan already partitions; "The plan rewrite" in
 * docs/super-sirius/distributed-sirius.md lists every plan shape and its exchange. Rank 0's
 * result is the query's; the other ranks' are empty, or meaningless after a gather.
 *
 * A HAVING over a grouped aggregate whose group key a Parquet scan reads also drops, before the
 * exchange, the partial groups already whole and failing it, when @p footers shows that no key
 * strictly inside a row group's key range is in another row group (see complete_group_having).
 *
 * Every rank must plan the same query: before returning, this blocks until every rank of
 * @p service has planned it, comparing the plans' shapes in an all-gather.
 *
 * Whatever it throws, it first aborts @p query on every rank and retires it on this one: the
 * query is numbered by then, and the other ranks would wait for this one in the all-gather.
 *
 * @param query This query's number, the same on every rank: its exchanges' stream ids.
 * @param lifecycle Gates the exchange sources' landing into their ports' repositories.
 * @param footers Reads a Parquet file's footer; may be empty.
 * @param stages Which stages run on rank 0 alone; by default none does.
 * @param directives What to do at the joins and grouped merges they name, instead of deciding
 *        it here; by default nothing.
 * @throws sirius::not_implemented_exception on an operator distributed execution does not
 *         support, rather than a wrong answer; this rank throws before the all-gather.
 * @throws sirius::invalid_input_exception when another rank planned a different shape, on every
 *         rank.
 */
void distribute_plan(duckdb::unique_ptr<op::sirius_physical_operator>& plan,
                     std::shared_ptr<exchange::exchange_service> service,
                     cucascade::memory::memory_space& gpu,
                     std::uint64_t query,
                     exec::query_lifecycle_registry& lifecycle,
                     parquet_footer_reader footers  = {},
                     deferred_scan_policy deferred  = {},
                     stage_policy stages            = {},
                     exchange_directives directives = {});

/// Bytes `[start, start + length)` of file `file` (an index into the table's files).
struct file_byte_range {
  std::size_t file     = 0;
  std::uint64_t start  = 0;
  std::uint64_t length = 0;

  bool operator==(file_byte_range const&) const = default;
};

/**
 * @brief The byte ranges rank @p rank of @p world scans, of files of @p file_bytes bytes each.
 *
 * The files are laid end to end and cut into @p world equal spans; rank r gets span r, as one
 * range per file it touches, in file order. The spans tile every file exactly, so with row groups
 * owned by the range holding their start offset (`parquet_byte_range.hpp`), every row group is
 * read by exactly one rank. A rank may get no row group at all, e.g. of a one-row-group table.
 */
[[nodiscard]] std::vector<file_byte_range> byte_ranges_for_rank(
  std::span<std::uint64_t const> file_bytes, std::size_t rank, std::size_t world);

/**
 * @brief Sets @p info's files to this rank's share of @p files, or to all of them when
 * @p op_params is not distributed.
 *
 * Every rank binds every file of a table. Local files are split by byte range
 * (`byte_ranges_for_rank`), so a rank may get no range at all; remote ones, which take no byte
 * ranges, are dealt whole: files rank, rank + world, ...
 *
 * @throws sirius::not_implemented_exception for a remote table of fewer files than ranks, or a
 *         scan that reads `filename` or `file_index`, which each rank would number from 0.
 */
void assign_files_to_rank(op::scan::parquet_ingestible_table_info& info,
                          std::vector<std::string> files,
                          operator_params const& op_params);

/**
 * @brief Refuses, in distributed mode, a scan that @p source would lower to a read no rank split:
 * a DuckDB table, `sirius_read_parquet` of one URI, or a Parquet scan whose plan carries its own
 * byte ranges. Each rank would read all of it, and the query's answer would count it once per rank.
 *
 * The plan generator calls it once per scan, before lowering, only when distributed; the other
 * refusals (`filename`/`file_index`, too few remote files) need the bound files and stay in
 * assign_files_to_rank.
 *
 * @throws sirius::not_implemented_exception naming what cannot be split.
 */
void check_distributed_scan(op::sirius_physical_table_scan const& scan,
                            connector const& source,
                            duckdb::ClientContext& context);

/// Has @p join merge its dynamic filters across ranks with @p merge, each of the @p ranks ranks
/// sizing its Blooms from its own build rows (op::merged_bloom_policy), and close its publication
/// when its build CONCAT (`children[1]`) finalizes:
/// a peer's merge waits on this rank's part, and that peer's probe may be what this rank's probe
/// waits on, so the join's own finalize is too late.
/**
 * @brief Whether a hashed join's build, sent whole to every rank, moves fewer bytes between ranks
 * than the exchanges it replaces.
 *
 * Whole, every rank sends its build rows to every other rank in place of one part in @p ranks of
 * them. That saves the join's Bloom merge: @p merged_keys filters, sized for @p build_rows, from
 * every rank to every other. It saves the probe's exchange as well, but by how much no rank knows
 * when the build must go: the probe rows reach it filtered, and the planner's estimate of them
 * has been 20 times too high and more. So the probe counts for nothing, and a join with no
 * merged filter (@p merged_keys of 0) is never worth it.
 */
[[nodiscard]] bool whole_build_saves_bytes(std::uint64_t build_bytes,
                                           std::uint64_t build_rows,
                                           std::uint64_t ranks,
                                           std::size_t merged_keys) noexcept;

void install_filter_merge(op::sirius_physical_hash_join& join,
                          std::shared_ptr<op::cross_rank_filter_merge> merge,
                          std::size_t ranks);

/// Has @p sample take its boundaries from every rank's sample, all-gathered on @p stream, and
/// give the range exchange's boundaries to @p sink, the EXCHANGE_SINK after it, in whose order
/// the samples are merged. @p sink must outlive @p sample's boundary task.
void install_sort_boundaries(op::sirius_physical_sort_sample& sample,
                             op::sirius_physical_exchange_sink& sink,
                             std::shared_ptr<exchange::exchange_service> service,
                             exchange::stream_id stream,
                             cucascade::memory::memory_space& gpu);

}  // namespace sirius::planner
