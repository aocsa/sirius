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
#include "op/sirius_physical_operator.hpp"

#include <cudf/table/table.hpp>
#include <cudf/types.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

namespace sirius::exchange {
class broadcast_choice;
class exchange_service;
}  // namespace sirius::exchange

namespace sirius::op {

/// How an EXCHANGE_SINK routes rows to ranks.
struct exchange_spec {
  enum class mode {
    hash,       ///< By a hash of key_columns.
    range,      ///< By key_columns against range boundaries, set before the first batch. Each
                ///< input batch must be sorted on key_columns, as ORDER_BY leaves it; each
                ///< slice stays sorted.
    broadcast,  ///< Every row to every rank.
    gather,     ///< Every row to rank `target`.
  };

  /// Distinct from PARTITION's seed, which partitions each rank's share afterwards.
  static constexpr std::uint32_t default_hash_seed = 0x9e3779b9u;

  mode routing = mode::hash;
  /// hash and range: the columns routed on.
  std::vector<int> key_columns;
  /// hash: per-key cast before hashing, as PARTITION's; empty = hash as-is.
  std::vector<cudf::data_type> key_cast_types;
  std::uint32_t hash_seed = default_hash_seed;
  /// hash: a row with a NULL in any key column goes to every rank, and so does one row of each
  /// sender's input, so a rank's share is empty only when every rank's is. A MARK join's build
  /// needs both: an unmatched probe row's mark is NULL when the build has a NULL key, or when the
  /// probe key is NULL and the build is not empty, on whichever rank. The extra copy of a keyed row
  /// never matches off its own rank: every probe row with its key hashes to that rank.
  bool null_keys_to_every_rank = false;
  /// range: the order of each key column.
  std::vector<cudf::order> column_order;
  std::vector<cudf::null_order> null_precedence;
  /// gather: the receiving rank.
  exchange::rank_t target = 0;
  /// hash: only ranks 0 to width - 1 receive rows; 0 = every rank. Every rank still gets the
  /// stream's end, so a rank that receives no row hands on its one empty batch.
  exchange::rank_t width = 0;
  /// hash: a join's run-time choice between this routing and a build sent whole to every rank
  /// (exchange::broadcast_choice); null = none. With counts_build this is the build side: the
  /// sink holds its input, counts it, and settles the choice when the input ends. Without, the
  /// probe side: its rows are hashed until the choice is known, and stay on their rank once the
  /// build went whole. Either way every rank's whole build meets each probe row once.
  std::shared_ptr<exchange::broadcast_choice> build_choice;
  bool counts_build = false;
};

/**
 * @brief Sends its pipeline's output to the ranks of an exchange: the sending half of an
 * exchange, paired with an EXCHANGE_SOURCE of the same stream on every rank.
 *
 * Each batch is split by the spec and each slice sent to its rank; this rank's own slice is
 * handed over without a copy. When the pipeline finishes, every rank that may receive is told the
 * stream has ended: all ranks, or for a gather only the target.
 *
 * In a plan it is the child of its EXCHANGE_SOURCE, so it ends the pipeline of its producer
 * subtree, and that pipeline feeds the source's (dataless) port.
 */
class sirius_physical_exchange_sink : public sirius_physical_operator {
 public:
  static constexpr SiriusPhysicalOperatorType TYPE = SiriusPhysicalOperatorType::EXCHANGE_SINK;

  /// @throws sirius::invalid_input_exception on a spec that does not fit @p types.
  sirius_physical_exchange_sink(duckdb::vector<sirius::logical_type> types,
                                std::size_t estimated_cardinality,
                                std::shared_ptr<exchange::exchange_service> service,
                                exchange::stream_id stream,
                                exchange_spec spec);

  bool is_sink() const override { return true; }

  [[nodiscard]] sirius::OrderPreservationType output_order_preservation() const override
  {
    return sirius::OrderPreservationType::NO_ORDER;
  }

  [[nodiscard]] exchange_spec const& spec() const noexcept { return _spec; }

  /// range: the world size - 1 boundary rows, over key_columns only, ascending in the spec's
  /// order; rank i gets rows from boundary i - 1 (inclusive) to boundary i (exclusive).
  void set_range_boundaries(std::shared_ptr<cudf::table const> boundaries);

  /// Splits each batch into its ranks' slices, as routed_operator_data. Done here rather than in
  /// sink(): an OOM here reschedules the task, while one in sink() fails the query, since sink()
  /// may already have sent earlier slices.
  std::unique_ptr<operator_data> execute(const operator_data& input_data,
                                         ::cuda::stream_ref stream) override;

  /// Sends each slice to its rank.
  void sink(const operator_data& input_data, ::cuda::stream_ref stream) override;

  /// An empty table shaped like the batches this sink's input produced, or null before the first
  /// one. A producer's batches can carry more columns than its declared types (a partial
  /// aggregate's AVG travels as a sum and a count), so this, not the types, is what an empty batch
  /// of the stream must look like.
  [[nodiscard]] std::unique_ptr<cudf::table> empty_like_input() const;

  [[nodiscard]] std::size_t no_history_peak_memory_estimate(
    const input_stats& stats) const override;

 protected:
  /// Ends the stream to every rank that may receive it.
  void on_finalize_operator() override;

 private:
  /// Where route() sends a batch: as the spec says or, once a join's build went whole to every
  /// rank, the build side's rows to every rank and the probe side's to this one.
  enum class route_to : std::uint8_t { spec, every_rank, this_rank };

  /// The slices of @p batch, one per rank; empty for a rank that gets none of it.
  std::vector<std::shared_ptr<cucascade::data_batch>> route(
    cucascade::read_only_data_batch const& batch,
    std::shared_ptr<cucascade::data_batch> const& handle,
    ::cuda::stream_ref stream,
    route_to where = route_to::spec);

  /// What execute() made of some input: batches, and the rank of each. A batch whose rank is the
  /// world size is a whole input batch, made beside its hashed slices while the build's choice
  /// was open: one or the other is sent.
  struct routed_batches {
    std::vector<std::shared_ptr<cucascade::data_batch>> batches;
    std::vector<std::size_t> destinations;
  };

  /// Sends each batch to its rank; with @p whole_build a whole input batch to every rank in
  /// place of its slices.
  void send(std::span<std::shared_ptr<cucascade::data_batch> const> batches,
            std::span<std::size_t const> destinations,
            bool whole_build);
  /// counts_build: settles the choice with the bytes this rank held, and sends what it held.
  void settle_build();
  /// Ends the stream to every rank that may receive it.
  void close_streams();

  std::shared_ptr<exchange::exchange_service> _service;
  exchange::stream_id _stream;
  exchange_spec _spec;
  std::mutex _boundaries_mutex;
  /// The ranks that receive rows: the first `width` of a hash exchange that has one, else all.
  [[nodiscard]] exchange::rank_t receivers() const noexcept;

  std::shared_ptr<cudf::table const> _boundaries;
  mutable std::mutex _input_schema_mutex;
  /// Empty, with the columns of the first input batch.
  std::unique_ptr<cudf::table> _input_schema;
  /// null_keys_to_every_rank: a row of this rank's input has gone to every rank.
  std::atomic<bool> _every_rank_has_a_row{false};
  /// counts_build: the input held while the choice is open, and its bytes.
  std::mutex _held_mutex;
  std::vector<routed_batches> _held;
  std::uint64_t _held_bytes = 0;
  /// counts_build: settles the choice and ends the stream once the input has ended. It waits for
  /// every rank, which the thread that finalizes a pipeline, under its status lock, must not.
  /// Last, so that it has ended before the members it uses go.
  std::jthread _settling;
};

}  // namespace sirius::op
