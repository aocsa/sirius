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

#include "planner/distributed_plan.hpp"

#include "cudf/cudf_utils.hpp"
#include "data/data_batch_utils.hpp"
#include "exchange/all_gather.hpp"
#include "exchange/exchange_service.hpp"
#include "exchange/wire.hpp"
#include "op/cudf_sort_order.hpp"
#include "op/distributed/bloom_all_gather_merge.hpp"
#include "op/distributed/sample_all_gather_boundaries.hpp"
#include "op/scan/parquet_scan_types.hpp"
#include "op/scan/scan_plan.hpp"
#include "op/sirius_physical_cte.hpp"
#include "op/sirius_physical_delim_join.hpp"
#include "op/sirius_physical_exchange_sink.hpp"
#include "op/sirius_physical_exchange_source.hpp"
#include "op/sirius_physical_filter.hpp"
#include "op/sirius_physical_gpu_values.hpp"
#include "op/sirius_physical_grouped_aggregate.hpp"
#include "op/sirius_physical_hash_join.hpp"
#include "op/sirius_physical_nested_loop_join.hpp"
#include "op/sirius_physical_partition.hpp"
#include "op/sirius_physical_projection.hpp"
#include "op/sirius_physical_sort_sample.hpp"
#include "op/sirius_physical_table_scan.hpp"
#include "planner/connector_registry.hpp"
#include "planner/gpu_admission.hpp"
#include "planner/substrait_scan_ranges.hpp"
#include "sirius/exception.hpp"
#include "sirius_config.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/table/table.hpp>

#include <cuda_runtime_api.h>

#include <cucascade/data/data_batch.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>

#include <algorithm>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace sirius::planner {
namespace {

using op::exchange_spec;
using op::SiriusPhysicalOperatorType;
using T = SiriusPhysicalOperatorType;

/// Where a subtree's rows are.
enum class placement {
  spread,  ///< Across ranks, each rank holding some.
  ranged,  ///< Across ranks, rank i holding the i-th range of a sort order.
  rank0,   ///< On rank 0; what other ranks hold is not part of the result.
};

/// One key of the hash exchange that spread a subtree's rows.
struct hashed_key {
  /// Every column whose value, cast to `type`, is what the exchange hashed.
  std::vector<int> columns;
  /// What the exchange hashed: the key's cast type, or else its column's type.
  cudf::data_type type;
};

/// Where a subtree's rows are, and for rows a hash exchange spread, which columns still hold its
/// keys.
struct distribution {
  placement where = placement::spread;
  /// The keys of the hash exchange that spread the rows, in its order. Rows that agree on one
  /// column of each key are on one rank. Empty when no column is known to hold some key; always
  /// empty unless spread.
  std::vector<hashed_key> hashed_on;

  /// Whether rows that agree on @p columns are on one rank: they include a column of every key.
  [[nodiscard]] bool co_located_on(std::vector<int> const& columns) const
  {
    return !hashed_on.empty() && std::ranges::all_of(hashed_on, [&](auto const& key) {
      return std::ranges::find_first_of(key.columns, columns) != key.columns.end();
    });
  }

  /// Whether every row is on the rank a hash exchange on @p keys, hashed as @p types, would send
  /// it to. The hash is of the key tuple, so the keys must match in number, order and type.
  [[nodiscard]] bool placed_by(std::vector<int> const& keys,
                               std::vector<cudf::data_type> const& types) const
  {
    if (hashed_on.size() != keys.size()) { return false; }
    for (std::size_t k = 0; k < keys.size(); ++k) {
      if (types[k].id() == cudf::type_id::EMPTY || types[k] != hashed_on[k].type ||
          std::ranges::find(hashed_on[k].columns, keys[k]) == hashed_on[k].columns.end()) {
        return false;
      }
    }
    return true;
  }
};

/// The type a hash exchange on column @p key of @p input hashes: @p cast unless EMPTY, else the
/// column's physical type. EMPTY when cuDF has no type for it.
cudf::data_type hashed_type(op::sirius_physical_operator const& input,
                            int key,
                            cudf::data_type cast)
{
  if (cast.id() != cudf::type_id::EMPTY) { return cast; }
  if (input.has_physical_overrides()) {
    return input.get_physical_types()[static_cast<std::size_t>(key)];
  }
  return get_cudf_type(input.types[static_cast<std::size_t>(key)])
    .value_or(cudf::data_type{cudf::type_id::EMPTY});
}

/// The types a hash exchange on @p keys of @p input hashes, through @p casts (empty: none).
std::vector<cudf::data_type> hashed_types(op::sirius_physical_operator const& input,
                                          std::vector<int> const& keys,
                                          std::vector<cudf::data_type> const& casts)
{
  std::vector<cudf::data_type> out;
  for (std::size_t k = 0; k < keys.size(); ++k) {
    out.push_back(hashed_type(
      input, keys[k], casts.empty() ? cudf::data_type{cudf::type_id::EMPTY} : casts[k]));
  }
  return out;
}

/// Spread by a hash exchange on @p keys, columns of its input, hashed as @p types. A cast is a
/// function of the key, so rows that agree on the key columns hash alike, whatever the casts.
distribution hashed_on(std::vector<int> const& keys, std::vector<cudf::data_type> const& types)
{
  distribution d;
  for (std::size_t k = 0; k < keys.size(); ++k) {
    d.hashed_on.push_back({.columns = {keys[k]}, .type = types[k]});
  }
  return d;
}

/// @p d over an operator's output, whose column j is input column sources[j] (-1: computed).
distribution through(distribution const& d, std::vector<int> const& sources)
{
  distribution out{.where = d.where};
  for (auto const& key : d.hashed_on) {
    auto& mapped = out.hashed_on.emplace_back(hashed_key{.type = key.type});
    for (int j = 0; j < static_cast<int>(sources.size()); ++j) {
      if (std::ranges::find(key.columns, sources[j]) != key.columns.end()) {
        mapped.columns.push_back(j);
      }
    }
    if (mapped.columns.empty()) { return {.where = d.where}; }
  }
  return out;
}

/// A grouped aggregate's output, its groups first, over its input @p d. A grouping set that leaves
/// out a key merges rows from every rank into one group, so then no column holds a key.
distribution grouped_output(distribution const& d, op::sirius_physical_grouped_aggregate const& agg)
{
  if (agg.has_grouping_sets()) { return {.where = d.where}; }
  return through(d, agg.group_idx);
}

/// A hash join's output over its @p probe and @p build inputs. When both are placed by a hash on
/// the join keys, key k of either side holds key k of the output. A side whose columns the join
/// fills with NULLs, for the other side's unmatched rows, holds no key: those rows are on the other
/// side's ranks.
distribution joined(op::sirius_physical_hash_join const& join,
                    distribution const& probe,
                    distribution const& build)
{
  using duckdb::JoinType;
  auto const type = join.join_type;
  // Probe columns, build columns, then a MARK join's mark.
  bool const has_probe_cols = type != JoinType::RIGHT_SEMI && type != JoinType::RIGHT_ANTI;
  bool const has_build_cols =
    type != JoinType::SEMI && type != JoinType::ANTI && type != JoinType::MARK;
  bool const pads_probe = type == JoinType::RIGHT || type == JoinType::OUTER;
  bool const pads_build =
    type == JoinType::LEFT || type == JoinType::OUTER || type == JoinType::SINGLE;
  std::vector<int> from_probe;
  std::vector<int> from_build;
  if (has_probe_cols) {
    for (auto const c : join.lhs_output_columns.col_idxs) {
      from_probe.push_back(pads_probe ? -1 : static_cast<int>(c));
      from_build.push_back(-1);
    }
  }
  if (has_build_cols) {
    for (auto const c : join.rhs_output_columns.col_idxs) {
      from_probe.push_back(-1);
      from_build.push_back(pads_build ? -1 : static_cast<int>(c));
    }
  }
  if (type == JoinType::MARK) {
    from_probe.push_back(-1);
    from_build.push_back(-1);
  }
  distribution out{.where = probe.where == placement::rank0 ? placement::rank0 : placement::spread};
  if (out.where != placement::spread || from_probe.size() != join.types.size()) { return out; }
  auto const p = through(probe, from_probe).hashed_on;
  auto const b = through(build, from_build).hashed_on;
  if (p.empty() || b.empty()) {
    out.hashed_on = p.empty() ? b : p;
    return out;
  }
  for (std::size_t k = 0; k < p.size(); ++k) {
    auto& key = out.hashed_on.emplace_back(p[k]);
    key.columns.insert(key.columns.end(), b[k].columns.begin(), b[k].columns.end());
  }
  return out;
}

/// A join's Bloom filters are sized for its estimated build rows, within these bounds: the same
/// size on every rank, since every rank plans the same estimate.
constexpr std::size_t min_bloom_keys = std::size_t{1} << 16;
constexpr std::size_t max_bloom_keys = std::size_t{1} << 30;
/// A hash join whose estimated build has fewer rows publishes no merged Bloom filters.
constexpr std::size_t min_merged_build_rows = std::size_t{1} << 14;
/// A join's build side is broadcast, and its probe side stays where it is, when the whole build
/// fits this budget on every GPU and sending it to every rank moves fewer bytes than hashing both
/// sides would.
constexpr std::uint64_t max_broadcast_build_bytes = std::uint64_t{1} << 30;
/// Width assumed for a variable-width column when estimating bytes.
constexpr std::uint64_t estimated_var_bytes = 32;

std::uint64_t estimated_bytes(op::sirius_physical_operator const& node)
{
  auto const width = estimate_bytes_per_row(node.types, estimated_var_bytes);
  auto const rows  = static_cast<std::uint64_t>(node.estimated_cardinality);
  return rows != 0 && width > std::numeric_limits<std::uint64_t>::max() / rows
           ? std::numeric_limits<std::uint64_t>::max()
           : rows * width;
}

/// The cuDF types of @p types; a type cuDF cannot hold is left out.
std::vector<cudf::data_type> physical_types_of(duckdb::vector<sirius::logical_type> const& types)
{
  std::vector<cudf::data_type> out;
  for (auto const& type : types) {
    if (auto const cudf_type = get_cudf_type(type)) { out.push_back(*cudf_type); }
  }
  return out;
}

/// Join types that never emit a build row without a probe match, so a build seen by every rank
/// yields each output row on one rank only.
bool emits_build_rows_only_with_probe(duckdb::JoinType type)
{
  return type != duckdb::JoinType::RIGHT && type != duckdb::JoinType::OUTER &&
         type != duckdb::JoinType::RIGHT_SEMI && type != duckdb::JoinType::RIGHT_ANTI;
}

struct rewriter {
  std::shared_ptr<exchange::exchange_service> service;
  cucascade::memory::memory_space* gpu;
  std::uint64_t query;
  exec::query_lifecycle_registry* lifecycle;
  std::uint32_t next_stream{1};
  /// The plan's shape, compared across ranks.
  std::string shape;
  /// Each CTE_SCAN's rows are where its CTE's materialization put them.
  std::map<op::sirius_physical_operator const*, placement> cte_scans;

  exchange::stream_id stream() { return exchange::make_stream_id(query, next_stream++); }

  op::sirius_physical_operator& expect(op::sirius_physical_operator& node, T type)
  {
    if (node.type != type) {
      throw sirius::internal_exception("distributed plan: expected {} under {}, found {}",
                                       op::SiriusPhysicalOperatorToString(type),
                                       node.get_name(),
                                       op::SiriusPhysicalOperatorToString(node.type));
    }
    return node;
  }

  /// Puts EXCHANGE_SOURCE → EXCHANGE_SINK(@p spec) in @p slot, above what was there.
  op::sirius_physical_exchange_sink& insert(duckdb::unique_ptr<op::sirius_physical_operator>& slot,
                                            exchange_spec spec,
                                            bool ordered)
  {
    // Refused before any rank runs: the wire would refuse the first batch. Grouped STDDEV_SAMP
    // partials are STRUCT columns, an ungrouped COUNT(DISTINCT) partial a LIST.
    for (auto const& type : slot->has_physical_overrides() ? slot->get_physical_types()
                                                           : physical_types_of(slot->types)) {
      if (!exchange::sendable(type)) {
        throw sirius::not_implemented_exception(
          "distributed execution: an exchange of a column of cuDF type id {}",
          static_cast<std::int32_t>(type.id()));
      }
    }
    auto child       = std::move(slot);
    auto const id    = stream();
    auto const types = child->types;
    auto const rows  = child->estimated_cardinality;
    std::set<exchange::rank_t> senders;
    if (spec.routing != exchange_spec::mode::gather || service->rank() == spec.target) {
      for (exchange::rank_t r = 0; r < service->world_size(); ++r) {
        senders.insert(r);
      }
    }
    shape += "X" + std::to_string(static_cast<int>(spec.routing)) + "/";
    for (auto const key : spec.key_columns) {
      shape += std::to_string(key) + ",";
    }
    auto sink = duckdb::make_uniq<op::sirius_physical_exchange_sink>(
      types, rows, service, id, std::move(spec));
    auto source = duckdb::make_uniq<op::sirius_physical_exchange_source>(
      types, rows, service, id, std::move(senders), ordered);
    source->set_query_lifecycle(*lifecycle);
    source->set_memory_space(*gpu);
    if (child->has_physical_overrides()) {
      sink->set_physical_types(child->get_physical_types());
      source->set_physical_types(child->get_physical_types());
    }
    auto& sink_ref = *sink;
    sink->children.push_back(std::move(child));
    source->children.push_back(std::move(sink));
    slot = std::move(source);
    return sink_ref;
  }

  /// A hash exchange on @p partition's keys, between it and its input. An input on rank 0 alone
  /// (a gathered aggregate) is spread from there; the other ranks' copies of it hold no rows.
  distribution exchange_below(op::sirius_physical_partition& partition,
                              bool null_keys_to_every_rank = false)
  {
    return hash_exchange(partition.children[0],
                         partition.partition_keys(),
                         partition.specification().key_cast_types,
                         null_keys_to_every_rank);
  }

  /// A hash exchange on @p keys through @p casts, above what is in @p slot.
  distribution hash_exchange(duckdb::unique_ptr<op::sirius_physical_operator>& slot,
                             std::vector<int> const& keys,
                             std::vector<cudf::data_type> const& casts,
                             bool null_keys_to_every_rank = false)
  {
    auto placed = hashed_on(keys, hashed_types(*slot, keys, casts));
    exchange_spec spec;
    spec.key_columns             = keys;
    spec.key_cast_types          = casts;
    spec.null_keys_to_every_rank = null_keys_to_every_rank;
    insert(slot, std::move(spec), false);
    return placed;
  }

  /// @p input, visited, unless already placed as a hash exchange on @p keys through @p casts would
  /// place it, then hash-exchanged on them. Null keys sent to every rank are never placed already.
  distribution hash_unless_placed(duckdb::unique_ptr<op::sirius_physical_operator>& input,
                                  std::vector<int> const& keys,
                                  std::vector<cudf::data_type> const& casts,
                                  bool null_keys_to_every_rank = false)
  {
    auto const placed = visit(input);
    if (!null_keys_to_every_rank && placed.placed_by(keys, hashed_types(*input, keys, casts))) {
      shape += "R";
      return placed;
    }
    return hash_exchange(input, keys, casts, null_keys_to_every_rank);
  }

  /// @p partition_node's input, hash-exchanged on its keys unless already placed so.
  distribution hash_below(op::sirius_physical_operator& partition_node,
                          bool null_keys_to_every_rank = false)
  {
    auto& partition = expect(partition_node, T::PARTITION).Cast<op::sirius_physical_partition>();
    return hash_unless_placed(partition.children[0],
                              partition.partition_keys(),
                              partition.specification().key_cast_types,
                              null_keys_to_every_rank);
  }

  /// After hash exchanges a rank's build holds only its share of the keys: a filter from it
  /// alone would drop rows other ranks' builds match, so every rank's Bloom is merged.
  void merge_filters_across_ranks(op::sirius_physical_hash_join& join)
  {
    if (!join.publishes_dynamic_filters()) { return; }
    auto const build_rows = join.children[1]->estimated_cardinality;
    // A right semi or anti join publishes before its probe side drains; a merge would make that
    // wait on every rank's build.
    if (build_rows < min_merged_build_rows || join.join_type == duckdb::JoinType::RIGHT_SEMI ||
        join.join_type == duckdb::JoinType::RIGHT_ANTI) {
      join.cancel_dynamic_filter_publication();
      shape += "f";
      return;
    }
    // One stream per admitted key: key k merges on first + k. The keys are fixed while planning,
    // so every rank reserves the same streams.
    auto const first = exchange::make_stream_id(query, next_stream);
    next_stream += static_cast<std::uint32_t>(join.dynamic_filter_plan().admitted_keys().size());
    install_filter_merge(join,
                         std::make_shared<op::bloom_all_gather_merge>(
                           service, first, join.dynamic_filter_plan().admitted_keys().size(), *gpu),
                         std::clamp(build_rows, min_bloom_keys, max_bloom_keys));
    shape += "F";
  }

  distribution visit(duckdb::unique_ptr<op::sirius_physical_operator>& slot)
  {
    auto& node = *slot;
    shape += op::SiriusPhysicalOperatorToString(node.type) + "(";
    auto const done = [&](distribution d) {
      shape += ")";
      return d;
    };
    switch (node.type) {
      case T::GPU_SCAN: return done({});
      // A join-edge dynamic filter: its filter holds every rank's keys (see HASH_JOIN).
      case T::DYNAMIC_FILTER: return done(visit(node.children[0]));

      case T::FILTER: {
        auto const input = visit(node.children[0]);
        auto const* kept = std::get_if<std::vector<cudf::size_type>>(
          &node.Cast<op::sirius_physical_filter>().output_columns);
        return done(kept ? through(input, {kept->begin(), kept->end()}) : input);
      }

      case T::PROJECTION: {
        auto const input = visit(node.children[0]);
        std::vector<int> sources;
        for (auto const& e : node.Cast<op::sirius_physical_projection>().select_list) {
          sources.push_back(e->holds<sirius::ast::reference>()
                              ? static_cast<int>(e->get<sirius::ast::reference>().column_index)
                              : -1);
        }
        return done(through(input, sources));
      }

      // A partial aggregate keeps its input's placement; its merge above decides the exchange.
      case T::HASH_GROUP_BY:
        return done(grouped_output(visit(node.children[0]),
                                   node.Cast<op::sirius_physical_grouped_aggregate>()));

      case T::HASH_JOIN: {
        // Both sides are CONCAT → PARTITION → input.
        auto& join        = node.Cast<op::sirius_physical_hash_join>();
        auto& probe       = expect(*expect(*node.children[0], T::CONCAT).children[0], T::PARTITION);
        auto& build       = expect(*expect(*node.children[1], T::CONCAT).children[0], T::PARTITION);
        auto const world  = static_cast<std::uint64_t>(service->world_size());
        auto const bbytes = estimated_bytes(*build.children[0]);
        // A BUILD_FILTER (right semi and anti joins only, which publish nothing distributed)
        // would publish this rank's keys alone, before the exchange: it goes.
        if (build.children[0]->type == T::BUILD_FILTER) {
          auto input        = std::move(build.children[0]->children[0]);
          build.children[0] = std::move(input);
        }
        if (emits_build_rows_only_with_probe(join.join_type) &&
            bbytes <= max_broadcast_build_bytes &&
            bbytes <= estimated_bytes(*probe.children[0]) / std::max<std::uint64_t>(world - 1, 1)) {
          // Every rank gets the whole build and keeps its probe rows. Its build being whole, its
          // Bloom filters hold every key, as on one GPU: nothing to merge.
          shape += "B";
          auto const probe_rows = visit(probe.children[0]);
          visit(build.children[0]);
          insert(
            build.children[0], exchange_spec{.routing = exchange_spec::mode::broadcast}, false);
          return done(joined(join, probe_rows, {}));
        }
        merge_filters_across_ranks(join);
        // Each side's rows meet their matches once both are exchanged on the join keys. A MARK
        // join's NULL build keys match nothing yet turn a missed probe row's mark NULL, so every
        // rank needs them; its sink also sends every rank one build row, so that a NULL probe
        // key's mark is NULL on any rank once the build has rows.
        auto const probe_rows = hash_below(probe);
        auto const build_rows = hash_below(build, join.join_type == duckdb::JoinType::MARK);
        return done(joined(join, probe_rows, build_rows));
      }

      case T::LEFT_DELIM_JOIN:
      case T::RIGHT_DELIM_JOIN: {
        // A delim join fans one input out to a side of its inner join (LEFT: the probe, RIGHT:
        // the build) and to the DISTINCT that the DELIM_SCANs replay. That input is hashed on the
        // delim columns before the fan-out, exactly as its join side would be: each rank's
        // DISTINCT then holds keys no other rank holds, and that join side needs no exchange.
        auto& delim = node.Cast<op::sirius_physical_delim_join>();
        if (!delim.join || delim.join->type != T::HASH_JOIN || delim.distinct == nullptr) {
          throw sirius::not_implemented_exception(
            "distributed execution: a delim join without a hash join");
        }
        auto& join  = delim.join->Cast<op::sirius_physical_hash_join>();
        auto& probe = expect(*expect(*join.children[0], T::CONCAT).children[0], T::PARTITION)
                        .Cast<op::sirius_physical_partition>();
        auto& build = expect(*expect(*join.children[1], T::CONCAT).children[0], T::PARTITION)
                        .Cast<op::sirius_physical_partition>();
        bool const left  = node.type == T::LEFT_DELIM_JOIN;
        auto& fanned     = left ? probe : build;
        auto& other      = left ? build : probe;
        auto const& keys = fanned.partition_keys();
        if (std::set<int>(keys.begin(), keys.end()) !=
            std::set<int>(delim.distinct->group_idx.begin(), delim.distinct->group_idx.end())) {
          throw sirius::not_implemented_exception(
            "distributed execution: a delim join whose join keys are not its delim columns");
        }
        hash_unless_placed(node.children[0], keys, fanned.specification().key_cast_types);
        merge_filters_across_ranks(join);
        hash_below(other);
        return done({});
      }

      // A rank's share of its delim join's DISTINCT (see LEFT_DELIM_JOIN).
      case T::DELIM_SCAN: return done({});

      // A literal table (VALUES, or a count DuckDB took from Parquet metadata): every rank planned
      // the same rows, so rank 0 keeps them and the other ranks emit none.
      case T::GPU_VALUES: {
        if (service->rank() != 0) {
          auto none = duckdb::make_uniq<op::sirius_physical_gpu_values>(
            node.types, node.estimated_cardinality, /*produce_single_row=*/false);
          if (node.has_physical_overrides()) {
            none->set_physical_types(node.get_physical_types());
          }
          slot = std::move(none);
        }
        return done({.where = placement::rank0});
      }

      case T::NESTED_LOOP_JOIN: {
        // NESTED_LOOP_JOIN → CONCAT → PARTITION → input, probe then build. Every rank keeps its
        // probe rows and gets the whole build side, so each pair meets once; an unmatched build
        // row would be emitted by every rank, so no join type that emits those.
        auto const join_type = node.Cast<op::sirius_physical_nested_loop_join>().join_type;
        if (!emits_build_rows_only_with_probe(join_type)) {
          throw sirius::not_implemented_exception("distributed execution: a {} nested-loop join",
                                                  duckdb::JoinTypeToString(join_type));
        }
        auto& probe = expect(*expect(*node.children[0], T::CONCAT).children[0], T::PARTITION);
        auto& build = expect(*expect(*node.children[1], T::CONCAT).children[0], T::PARTITION);
        auto const probe_rows = visit(probe.children[0]).where;
        visit(build.children[0]);
        insert(build.children[0], exchange_spec{.routing = exchange_spec::mode::broadcast}, false);
        return done(
          {.where = probe_rows == placement::rank0 ? placement::rank0 : placement::spread});
      }

      case T::DENSE_COUNT_JOIN: {
        // DENSE_COUNT_JOIN → PARTITION → input, both sides: exchanged on the join key, a rank
        // counts the preserved keys it holds, every match of which is on it too.
        for (auto& side : node.children) {
          hash_below(*side);
        }
        return done({});
      }

      case T::CTE: {
        // CTE → (materialization, consumer): each rank materializes its share, and the CTE_SCANs
        // under the consumer read it where it is.
        auto const materialized = visit(node.children[0]).where;
        for (auto const& scan : node.Cast<op::sirius_physical_cte>().cte_scans) {
          cte_scans[&scan.get()] = materialized;
        }
        return done(visit(node.children[1]));
      }

      case T::CTE_SCAN: return done({.where = cte_scans.at(&node)});

      case T::MERGE_GROUP_BY: {
        // MERGE_GROUP_BY → PARTITION → HASH_GROUP_BY: partial results meet by group key. Rows
        // already hashed on some of the group keys hold whole groups on each rank, and merge
        // there.
        auto& partition =
          expect(*node.children[0], T::PARTITION).Cast<op::sirius_physical_partition>();
        auto& grouped = expect(*partition.children[0], T::HASH_GROUP_BY)
                          .Cast<op::sirius_physical_grouped_aggregate>();
        if (grouped.has_count_distinct) {
          // Partial distinct sets are LIST columns, which the exchange does not carry: the input
          // rows meet by group key instead, and each rank's partials hold whole groups.
          if (grouped.has_grouping_sets()) {
            throw sirius::not_implemented_exception(
              "distributed execution: COUNT(DISTINCT) with grouping sets");
          }
          auto input = visit(grouped.children[0]);
          if (!input.co_located_on(grouped.group_idx)) {
            input = hash_exchange(grouped.children[0], grouped.group_idx, {});
          }
          return done(grouped_output(input, grouped));
        }
        auto const partials = visit(partition.children[0]);
        if (partials.co_located_on(partition.partition_keys())) { return done(partials); }
        auto const merged = exchange_below(partition);
        // The keys then include the grouping set id, which the merge does not output.
        return done(grouped.has_grouping_sets() ? distribution{} : merged);
      }

      case T::MERGE_AGGREGATE: {
        // MERGE_AGGREGATE → UNGROUPED_AGGREGATE: partial results gathered to rank 0.
        auto& partial = expect(*node.children[0], T::UNGROUPED_AGGREGATE);
        if (visit(partial.children[0]).where == placement::rank0) {
          return done({.where = placement::rank0});
        }
        insert(node.children[0], exchange_spec{.routing = exchange_spec::mode::gather}, false);
        return done({.where = placement::rank0});
      }

      case T::MERGE_TOP_N: {
        // MERGE_TOP_N → TOP_N: each rank's top rows gathered to rank 0, whose merge keeps the
        // overall top. A row outside its rank's top N is outside the overall top N too.
        auto& partial = expect(*node.children[0], T::TOP_N);
        if (visit(partial.children[0]).where == placement::rank0) {
          return done({.where = placement::rank0});
        }
        insert(node.children[0], exchange_spec{.routing = exchange_spec::mode::gather}, false);
        return done({.where = placement::rank0});
      }

      case T::MERGE_SORT: {
        // MERGE_SORT → SORT_PARTITION → SORT_SAMPLE → ORDER_BY → input: rows range-exchanged
        // after the sample, on boundaries every rank's sample agrees on.
        auto& partition = expect(*node.children[0], T::SORT_PARTITION);
        auto& sample =
          expect(*partition.children[0], T::SORT_SAMPLE).Cast<op::sirius_physical_sort_sample>();
        auto& order = expect(*sample.children[0], T::ORDER_BY);
        if (visit(order.children[0]).where == placement::rank0) {
          return done({.where = placement::rank0});
        }
        exchange_spec spec;
        spec.routing = exchange_spec::mode::range;
        for (auto const& o : sample.orders) {
          if (o.expression->GetExpressionClass() != duckdb::ExpressionClass::BOUND_REF) {
            throw sirius::not_implemented_exception(
              "distributed execution: ORDER BY on an expression");
          }
          spec.key_columns.push_back(
            static_cast<int>(o.expression->Cast<duckdb::BoundReferenceExpression>().Index()));
          spec.column_order.push_back(op::to_cudf_order(o.type));
          spec.null_precedence.push_back(op::to_cudf_null_order(o.type, o.null_order));
        }
        // Its own statement: the exchange takes its stream id before the all-gather takes one.
        auto& sink = insert(partition.children[0], std::move(spec), false);
        // The sample, now below the sink, takes its boundaries from every rank's sample.
        install_sort_boundaries(sample, sink, service, stream(), *gpu);
        return done({.where = placement::ranged});
      }

      default:
        throw sirius::not_implemented_exception("distributed execution does not support {}",
                                                op::SiriusPhysicalOperatorToString(node.type));
    }
  }
};

/// Fails the query on every rank unless every rank planned the same shape.
void require_same_shape(rewriter& r)
{
  auto const hash   = static_cast<std::int64_t>(std::hash<std::string>{}(r.shape));
  auto const stream = r.gpu->acquire_stream();
  auto column       = cudf::make_numeric_column(cudf::data_type{cudf::type_id::INT64},
                                          1,
                                          cudf::mask_state::UNALLOCATED,
                                          stream,
                                          r.gpu->get_default_allocator());
  RMM_CUDA_TRY(cudaMemcpyAsync(column->mutable_view().data<std::int64_t>(),
                               &hash,
                               sizeof(hash),
                               cudaMemcpyHostToDevice,
                               stream.get()));
  // The exchange sends from its own stream.
  stream.sync();
  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(column));
  auto mine = sirius::make_data_batch(std::make_unique<cudf::table>(std::move(columns)),
                                      *r.gpu,
                                      stream,
                                      telemetry::batch_telemetry_info{});
  // The query's last stream id: the rewrite numbers its exchanges up from 1.
  auto const all = exchange::all_gather(
    *r.service,
    exchange::make_stream_id(r.query, (std::uint64_t{1} << exchange::query_stream_bits) - 1),
    mine,
    *r.gpu,
    stream.get());
  std::vector<std::int64_t> theirs(all.size());
  // Held until the copies finish, so nothing spills the hashes meanwhile.
  std::vector<decltype(all.front()->to_read_only())> held;
  for (exchange::rank_t rank = 0; rank < all.size(); ++rank) {
    auto const& ro = held.emplace_back(all[rank]->to_read_only());
    RMM_CUDA_TRY(cudaMemcpyAsync(&theirs[rank],
                                 get_cudf_table_view(ro).column(0).data<std::int64_t>(),
                                 sizeof(std::int64_t),
                                 cudaMemcpyDeviceToHost,
                                 stream.get()));
  }
  stream.sync();
  for (exchange::rank_t rank = 0; rank < all.size(); ++rank) {
    if (theirs[rank] != hash) {
      throw sirius::invalid_input_exception(
        "distributed execution: rank {} planned this query differently from rank {}",
        rank,
        r.service->rank());
    }
  }
}

}  // namespace

void distribute_plan(duckdb::unique_ptr<op::sirius_physical_operator>& plan,
                     std::shared_ptr<exchange::exchange_service> service,
                     cucascade::memory::memory_space& gpu,
                     std::uint64_t query,
                     exec::query_lifecycle_registry& lifecycle)
{
  rewriter r{service, &gpu, query, &lifecycle};
  try {
    auto const where = r.visit(plan).where;
    if (where != placement::rank0) {
      r.insert(plan,
               exchange_spec{.routing = exchange_spec::mode::gather},
               /*ordered=*/where == placement::ranged);
    }
    require_same_shape(r);
  } catch (...) {
    // The query is numbered, so a peer that planned it may already wait for this rank in the
    // shape all-gather, and nothing else tells it: no window runs a plan that failed here. This
    // rank never runs the number, so it retires it too. Best effort: the failure itself goes on.
    std::string what = "unknown error";
    try {
      throw;
    } catch (std::exception const& e) {
      what = e.what();
    } catch (...) {
    }
    try {
      service->abort(query, what);
      service->forget(query);
    } catch (...) {
    }
    throw;
  }
}

std::vector<file_byte_range> byte_ranges_for_rank(std::span<std::uint64_t const> file_bytes,
                                                  std::size_t rank,
                                                  std::size_t world)
{
  std::uint64_t total = 0;
  for (auto const bytes : file_bytes) {
    total += bytes;
  }
  // floor(total * r / world), without the product overflowing.
  auto const cut = [total, world](std::size_t r) {
    return total / world * r + total % world * r / world;
  };
  auto const begin = cut(rank);
  auto const end   = cut(rank + 1);
  std::vector<file_byte_range> mine;
  std::uint64_t offset = 0;
  for (std::size_t f = 0; f < file_bytes.size(); offset += file_bytes[f], ++f) {
    auto const from = std::max(begin, offset);
    auto const to   = std::min(end, offset + file_bytes[f]);
    if (from < to) { mine.push_back({f, from - offset, to - from}); }
  }
  return mine;
}

void assign_files_to_rank(op::scan::parquet_ingestible_table_info& info,
                          std::vector<std::string> files,
                          operator_params const& op_params)
{
  if (!op_params.distributed()) {
    info.resolved_file_paths = std::move(files);
    return;
  }
  // A rank numbers the files of its own share from 0, so their index and a filter on their name
  // would mean different files on different ranks.
  using kind              = op::scan::scan_plan::parquet_virtual_column_kind;
  auto const names_a_file = [&](auto const& column) {
    return column.HasPrimaryIndex() &&
           std::any_of(
             info.virtual_columns.begin(), info.virtual_columns.end(), [&](auto const& v) {
               return v.column_id == column.GetPrimaryIndex() &&
                      (v.kind == kind::FILENAME || v.kind == kind::FILE_INDEX);
             });
  };
  if (std::any_of(info.column_ids.begin(), info.column_ids.end(), names_a_file)) {
    throw sirius::not_implemented_exception(
      "distributed execution: filename or file_index of a table split across ranks");
  }
  auto const world  = op_params.exchange_peers.size();
  auto const rank   = op_params.exchange_rank;
  auto const remote = std::any_of(
    files.begin(), files.end(), [](auto const& f) { return f.find("://") != std::string::npos; });
  if (remote) {
    // TODO(#2377): split remote files by byte range too, as local ones are.
    if (files.size() < world) {
      throw sirius::not_implemented_exception(
        "distributed execution: a remote table of {} files across {} ranks; each rank needs one. "
        "Split it into at least {} files, or read it from local files, which split by byte range",
        files.size(),
        world,
        world);
    }
    for (std::size_t i = rank; i < files.size(); i += world) {
      info.resolved_file_paths.push_back(std::move(files[i]));
    }
    return;
  }
  std::vector<std::uint64_t> bytes;
  bytes.reserve(files.size());
  for (auto const& f : files) {
    bytes.push_back(std::filesystem::file_size(f));
  }
  for (auto const& r : byte_ranges_for_rank(bytes, rank, world)) {
    info.resolved_file_paths.push_back(std::move(files[r.file]));
    info.resolved_file_ranges.push_back({{r.start, r.length}});
  }
}

void check_distributed_scan(op::sirius_physical_table_scan const& scan,
                            connector const& source,
                            duckdb::ClientContext& context)
{
  // Nothing splits a DuckDB table by rank: every rank would scan all of it.
  if (source.kind == op::scan::source_kind::duckdb_native) {
    throw sirius::not_implemented_exception(
      "distributed execution: a DuckDB table; read the table from Parquet files instead");
  }
  // One URI, expanded only when read: nothing splits it, so every rank would read all of it.
  if (scan.function.GetName() == "sirius_read_parquet") {
    throw sirius::not_implemented_exception(
      "distributed execution: read_parquet of one remote URI; every rank would read all of it");
  }
  // The plan's own ranges would replace this rank's share (lower_parquet_scan attaches them).
  if (source.lower == &lower_parquet_scan && context.registered_state &&
      context.registered_state->Get<scan_byte_ranges_state>(scan_byte_ranges_state::kStateKey)) {
    throw sirius::not_implemented_exception(
      "distributed execution: a plan that carries its own byte ranges");
  }
}

void install_filter_merge(op::sirius_physical_hash_join& join,
                          std::shared_ptr<op::cross_rank_filter_merge> merge,
                          std::size_t bloom_keys)
{
  join.dynamic_filter_session().set_cross_rank_merge(
    std::move(merge), op::merged_bloom_policy(join.dynamic_filter_plan().policy(), bloom_keys));
  // finish_input() never waits: a publication not yet started leaves each key's merge, and one
  // already running completes on its own.
  join.children[1]->add_finalize_listener(
    [&session = join.dynamic_filter_session()] { session.finish_input(); });
}

void install_sort_boundaries(op::sirius_physical_sort_sample& sample,
                             op::sirius_physical_exchange_sink& sink,
                             std::shared_ptr<exchange::exchange_service> service,
                             exchange::stream_id stream,
                             cucascade::memory::memory_space& gpu)
{
  sample.set_cross_rank_boundaries(std::make_shared<op::sample_all_gather_boundaries>(
    std::move(service),
    stream,
    gpu,
    sink.spec().column_order,
    sink.spec().null_precedence,
    [&sink](auto boundaries) { sink.set_range_boundaries(std::move(boundaries)); }));
}
}  // namespace sirius::planner
