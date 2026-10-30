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

#include "exchange/exchange_service.hpp"
#include "query_id.hpp"

#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace sirius {
struct operator_params;
namespace memory {
class sirius_memory_reservation_manager;
}
}  // namespace sirius

namespace sirius::exchange {

/**
 * @brief This process's part in distributed execution: its exchange, the numbering of its
 * distributed queries, and the execution window each one runs in.
 */
class distributed_session {
 public:
  /// \brief This process's exchange, connected to every peer of @p params on first use.
  /// @throws sirius::invalid_input_exception when the settings change after it connected, or
  ///         Sirius was built without NIXL.
  [[nodiscard]] std::shared_ptr<exchange_service> get_exchange_service(
    operator_params const& params, memory::sirius_memory_reservation_manager& memory);

  /// \brief Refuses, once connected, other peers or another rank than this process connected
  /// with. A connection that sets them anew sets the same ones.
  /// @throws sirius::invalid_input_exception
  void require_peers(std::vector<std::string> const& peers);
  void require_rank(std::uint32_t rank);

  /// \brief Numbers a distributed query planned on @p connection, from 1, and ties it to
  /// @p active_window unless that is 0. Every rank runs the same statements, so the same query
  /// gets the same number, and its exchanges the same stream ids. That holds for one statement
  /// stream only, so one connection per process runs distributed queries.
  /// @throws sirius::invalid_input_exception when another connection already runs them.
  [[nodiscard]] std::uint64_t next_query(std::uint64_t connection, std::uint64_t active_window);

  /// \brief @p connection is closing: if it runs distributed queries, the next connection to plan
  /// one takes over, and numbering continues where it stopped.
  void release_connection(std::uint64_t connection);

  /// \brief Window @p window runs distributed query @p query, until close_window().
  void open_window(query_id_t window, std::uint64_t query);
  void close_window(query_id_t window);

  /// \brief The distributed query execution window @p window runs, or 0.
  [[nodiscard]] std::uint64_t query_of(query_id_t window);

  /// \brief Window @p window's query failed with @p error: if it is a distributed query, every
  /// peer's run of it fails too, rather than waiting for this rank's batches.
  void abort(query_id_t window, std::exception_ptr const& error) noexcept;

  /// \brief Waits until the exchange no longer holds batches window @p window's query sent.
  void wait_sent(query_id_t window);

 private:
  std::mutex mutex_;
  std::vector<std::string> peers_;
  std::uint32_t rank_{0};
  std::shared_ptr<exchange_service> service_;
  std::uint64_t queries_{0};
  /// The connection that runs this process's distributed queries.
  std::optional<std::uint64_t> connection_;
  /// Each open execution window's distributed query. A window may retire while another
  /// connection's window runs, so it names its own.
  std::unordered_map<std::uint64_t, std::uint64_t> query_by_window_;
};

}  // namespace sirius::exchange
