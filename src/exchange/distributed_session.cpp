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

#include "exchange/distributed_session.hpp"

#include "exchange/exchange_runtime.hpp"
#include "log/logging.hpp"
#include "memory/sirius_memory_reservation_manager.hpp"
#include "sirius/exception.hpp"
#include "sirius_config.hpp"

#include <chrono>

namespace sirius::exchange {

std::shared_ptr<exchange_service> distributed_session::get_exchange_service(
  operator_params const& params, memory::sirius_memory_reservation_manager& memory)
{
  std::lock_guard lock{mutex_};
  if (service_) {
    if (params.exchange_peers != peers_ || params.exchange_rank != rank_) {
      throw invalid_input_exception(
        "sirius_exchange_peers and sirius_exchange_rank cannot change once connected");
    }
    return service_;
  }
  if (params.exchange_rank >= params.exchange_peers.size()) {
    throw invalid_input_exception(
      "sirius_exchange_rank {} of {} peers", params.exchange_rank, params.exchange_peers.size());
  }
  auto* gpu  = memory.get_memory_space(cucascade::memory::Tier::GPU, 0);
  auto* host = memory.get_memory_space(cucascade::memory::Tier::HOST, 0);
  service_   = make_nixl_exchange(params.exchange_rank, params.exchange_peers, *gpu, host);
  peers_     = params.exchange_peers;
  rank_      = params.exchange_rank;
  return service_;
}

std::uint64_t distributed_session::next_query(std::uint64_t connection, std::uint64_t active_window)
{
  std::lock_guard lock{mutex_};
  if (!connection_) { connection_ = connection; }
  if (*connection_ != connection) {
    throw invalid_input_exception(
      "distributed execution runs on one connection per process: connection {} runs it, so "
      "connection {} cannot",
      *connection_,
      connection);
  }
  auto const query = ++queries_;
  if (active_window != 0) { query_by_window_[active_window] = query; }
  return query;
}

void distributed_session::release_connection(std::uint64_t connection)
{
  std::lock_guard lock{mutex_};
  if (connection_ == connection) { connection_.reset(); }
}

void distributed_session::open_window(query_id_t window, std::uint64_t query)
{
  std::lock_guard lock{mutex_};
  query_by_window_[value_of(window)] = query;
}

void distributed_session::close_window(query_id_t window)
{
  std::lock_guard lock{mutex_};
  query_by_window_.erase(value_of(window));
}

std::uint64_t distributed_session::query_of(query_id_t window)
{
  std::lock_guard lock{mutex_};
  auto const it = query_by_window_.find(value_of(window));
  return it == query_by_window_.end() ? 0 : it->second;
}

void distributed_session::abort(query_id_t window, std::exception_ptr const& error) noexcept
{
  try {
    auto const query = query_of(window);
    if (query == 0) { return; }
    std::shared_ptr<exchange_service> service;
    {
      std::lock_guard lock{mutex_};
      service = service_;
    }
    if (!service) { return; }
    std::string what = "unknown error";
    try {
      std::rethrow_exception(error);
    } catch (std::exception const& e) {
      what = e.what();
    } catch (...) {
    }
    SIRIUS_LOG_WARN("Distributed query {} failed; aborting it on every rank: {}", query, what);
    service->abort(query, what);
  } catch (...) {  // best effort: the peers' own timeouts remain
  }
}

void distributed_session::wait_sent(query_id_t window)
{
  if (auto const query = query_of(window); query != 0) {
    std::shared_ptr<exchange_service> service;
    {
      std::lock_guard lock{mutex_};
      service = service_;
    }
    if (service && !service->wait_sent(query, std::chrono::minutes{1})) {
      SIRIUS_LOG_WARN("Distributed query {}: sent batches still held by the exchange after 1 min",
                      query);
    }
  }
}

}  // namespace sirius::exchange
