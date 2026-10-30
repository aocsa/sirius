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

#include "exchange/transport.hpp"
#include "op/sirius_physical_sort_sample.hpp"

#include <cudf/table/table.hpp>
#include <cudf/types.hpp>

#include <functional>
#include <memory>
#include <vector>

namespace sirius::exchange {
class exchange_service;
}

namespace sirius::op {

/**
 * @brief SORT_SAMPLE across the ranks of a range exchange.
 *
 * Every rank shares an evenly spaced summary of its sample, and merges all of them in rank
 * order, so every rank holds the same global sample. From it each derives the same world size
 * - 1 boundaries for the range exchange after this operator, and its own partition boundaries
 * from the part of the sample inside its range. Every rank's boundary task joins the exchange,
 * one with no input included.
 */
class sirius_physical_distributed_sort_sample : public sirius_physical_sort_sample {
 public:
  struct distributed_sampling {
    std::shared_ptr<exchange::exchange_service> service;
    /// The all-gather's stream id, the same on every rank.
    exchange::stream_id stream{0};
    /// Where gathered summaries are merged.
    cucascade::memory::memory_space* gpu{nullptr};
    /// Receives the range exchange's boundaries.
    std::function<void(std::shared_ptr<cudf::table const>)> set_exchange_boundaries;
  };

  //! Rows of each rank's sample summary shared with the other ranks.
  static constexpr std::size_t distributed_summary_rows = 1024;

  //! Takes @p local's types, orders, estimate and settings, not its children.
  sirius_physical_distributed_sort_sample(sirius_physical_sort_sample const& local,
                                          distributed_sampling sampling);

  std::unique_ptr<operator_data> execute(const operator_data& input_data,
                                         ::cuda::stream_ref stream) override;

  std::unique_ptr<operator_data> get_next_task_input_data() override;

  //! The boundary task is still owed, input or not, until it has been claimed.
  [[nodiscard]] bool all_ports_empty() override;
  //! get_next_task_input_data() has already claimed the task (SCHEDULED) when this is asked.
  [[nodiscard]] bool runs_without_input() const override
  {
    return _boundary_state.load(std::memory_order_acquire) != BoundaryState::DONE;
  }

 protected:
  //! Shares @p sample_keys (sorted, possibly empty), then sets both sets of boundaries from the
  //! global sample; this rank keeps up to @p num_parts partitions.
  void compute_boundaries(cudf::table_view sample_keys,
                          size_t num_parts,
                          cucascade::memory::memory_space& space,
                          ::cuda::stream_ref stream) override;

 private:
  distributed_sampling _sampling;
  std::vector<cudf::order> _column_order;
  std::vector<cudf::null_order> _null_precedence;
};

}  // namespace sirius::op
