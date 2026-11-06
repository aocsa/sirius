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

#include "downgrade/downgrade_executor.hpp"
#include "exchange/exchange_runtime.hpp"
#include "log/logging.hpp"
#include "memory/sirius_memory_reservation_manager.hpp"
#include "sirius/exception.hpp"
#include "sirius_config.hpp"

#include <algorithm>
#include <chrono>

namespace sirius::exchange {

std::shared_ptr<exchange_service> distributed_session::get_exchange_service(
  operator_params const& params,
  memory::sirius_memory_reservation_manager& memory,
  std::vector<std::unique_ptr<parallel::downgrade_executor>> const& downgrade)
{
  std::lock_guard lock{mutex_};
  if (service_) {
    if (params.exchange_peers != peers_ || params.exchange_rank != rank_ ||
        params.exchange_host_staging != host_staging_) {
      throw invalid_input_exception(
        "sirius_exchange_peers, sirius_exchange_rank and sirius_exchange_host_staging cannot "
        "change once connected");
    }
    return service_;
  }
  if (params.exchange_rank >= params.exchange_peers.size()) {
    throw invalid_input_exception(
      "sirius_exchange_rank {} of {} peers", params.exchange_rank, params.exchange_peers.size());
  }
  auto* gpu  = memory.get_memory_space(cucascade::memory::Tier::GPU, 0);
  auto* host = memory.get_memory_space(cucascade::memory::Tier::HOST, 0);
  auto const executor =
    std::ranges::find(downgrade, gpu->get_id(), [](auto const& e) { return e->get_space_id(); });
  if (executor == downgrade.end()) {
    throw internal_exception("exchange: no downgrade executor for the GPU space");
  }
  nixl_exchange_options opts;
  opts.staging = parse_host_staging(params.exchange_host_staging);
  // A batch with no room asks the GPU's downgrade executor for it, as a task would. terminate()
  // shuts the exchange down before the executors stop.
  opts.transport_options.request_room = [&executor = **executor, gpu](std::size_t bytes) {
    static_cast<void>(
      executor.request_downgrade([gpu, bytes] { return gpu->get_available_memory() >= bytes; }));
  };
  service_      = make_nixl_exchange(params.exchange_rank, params.exchange_peers, *gpu, host, opts);
  peers_        = params.exchange_peers;
  rank_         = params.exchange_rank;
  host_staging_ = params.exchange_host_staging;
  return service_;
}

void distributed_session::require_peers(std::vector<std::string> const& peers)
{
  std::lock_guard lock{mutex_};
  if (service_ && peers != peers_) {
    throw invalid_input_exception("sirius_exchange_peers cannot change once connected");
  }
}

void distributed_session::require_rank(std::uint32_t rank)
{
  std::lock_guard lock{mutex_};
  if (service_ && rank != rank_) {
    throw invalid_input_exception("sirius_exchange_rank cannot change once connected");
  }
}

void distributed_session::require_host_staging(std::string const& staging)
{
  std::lock_guard lock{mutex_};
  if (service_ && staging != host_staging_) {
    throw invalid_input_exception("sirius_exchange_host_staging cannot change once connected");
  }
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
    if (service) { service->forget(query); }
  }
}

void distributed_session::shutdown()
{
  std::shared_ptr<exchange_service> service;
  {
    std::lock_guard lock{mutex_};
    service = std::move(service_);
  }
  if (service) { service->shutdown(); }
}

}  // namespace sirius::exchange
