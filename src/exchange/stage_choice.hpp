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

#include <cuda/stream>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>

namespace cucascade::memory {
class memory_space;
}  // namespace cucascade::memory

namespace sirius::exchange {

class exchange_service;

/**
 * @brief The bytes every rank holds for one stage, summed alike on every rank.
 *
 * A rank that holds its stage's input to the end reports its bytes once, with total(), and gets
 * the sum over all ranks. A rank that will not hold its input to the end calls over() instead: it
 * waits for no one, and then no rank gets a sum. One all-gather on the stream carries the counts,
 * so every rank must end in total() or over() on the same stream id.
 */
class stage_count {
 public:
  static constexpr std::chrono::milliseconds default_timeout = std::chrono::minutes{10};

  stage_count(std::shared_ptr<exchange_service> service,
              stream_id stream,
              cucascade::memory::memory_space& gpu,
              std::chrono::milliseconds timeout = default_timeout);

  /// This rank gives no count, so no rank gets a sum. Does not wait; does nothing once this rank
  /// has an answer or is getting one.
  void over();

  /**
   * @brief This rank's bytes; blocks until every rank has reported or gone over.
   *
   * Never call it on the transport's engine thread (see all_gather). A repeated call, as from a
   * task that runs again, returns the first call's answer without another all-gather.
   *
   * @return The sum over all ranks, or nothing when a rank went over.
   * @throws the all-gather's error, on this call and on every later one.
   */
  [[nodiscard]] std::optional<std::uint64_t> total(std::uint64_t mine, ::cuda::stream_ref stream);

  /// Whether this rank has its answer.
  [[nodiscard]] bool resolved() const;

  /// The answer another thread of this rank gets from total() or over(); blocks until it has it.
  /// @throws as total(), or sirius::internal_exception after the timeout.
  [[nodiscard]] std::optional<std::uint64_t> wait() const;

 private:
  enum class state : std::uint8_t { open, gathering, resolved };

  [[nodiscard]] std::optional<std::uint64_t> answer(std::unique_lock<std::mutex>& lock) const;
  [[nodiscard]] std::optional<std::uint64_t> gather(std::uint64_t mine, ::cuda::stream_ref stream);

  std::shared_ptr<exchange_service> _service;
  stream_id _stream;
  cucascade::memory::memory_space* _gpu;
  std::chrono::milliseconds _timeout;
  mutable std::mutex _mutex;
  mutable std::condition_variable _changed;
  state _state = state::open;
  std::optional<std::uint64_t> _sum;
  std::exception_ptr _error;
};

}  // namespace sirius::exchange
