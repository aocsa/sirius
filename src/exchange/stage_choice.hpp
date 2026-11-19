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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

namespace cucascade::memory {
class memory_space;
}  // namespace cucascade::memory

namespace sirius::exchange {

class exchange_service;

/// What a rank holds for a stage, or all ranks together.
struct stage_load {
  std::uint64_t bytes = 0;
  std::uint64_t rows  = 0;
};

/**
 * @brief The bytes and rows every rank holds for one stage, summed alike on every rank.
 *
 * A rank that holds its stage's input to the end reports what it holds once, with total(), and
 * gets the sum over all ranks. A rank that will not hold its input to the end calls over()
 * instead: it waits for no one, and then no rank gets a sum. One all-gather on the stream carries
 * the counts, so every rank must end in total() or over() on the same stream id.
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
   * @brief What this rank holds; blocks until every rank has reported or gone over.
   *
   * Never call it on the transport's engine thread (see all_gather). A repeated call, as from a
   * task that runs again, returns the first call's answer without another all-gather.
   *
   * @return The sum over all ranks, or nothing when a rank went over.
   * @throws the all-gather's error, on this call and on every later one.
   */
  [[nodiscard]] std::optional<stage_load> total(stage_load mine, ::cuda::stream_ref stream);

  /// Whether this rank has its answer.
  [[nodiscard]] bool resolved() const;

  /// The GPU the counts are gathered on.
  [[nodiscard]] cucascade::memory::memory_space& gpu() const noexcept { return *_gpu; }

  /// The answer another thread of this rank gets from total() or over(); blocks until it has it.
  /// @throws as total(), or sirius::internal_exception after the timeout.
  [[nodiscard]] std::optional<stage_load> wait() const;

 private:
  enum class state : std::uint8_t { open, gathering, resolved };

  [[nodiscard]] std::optional<stage_load> answer(std::unique_lock<std::mutex>& lock) const;
  [[nodiscard]] std::optional<stage_load> gather(stage_load mine, ::cuda::stream_ref stream);

  std::shared_ptr<exchange_service> _service;
  stream_id _stream;
  cucascade::memory::memory_space* _gpu;
  std::chrono::milliseconds _timeout;
  mutable std::mutex _mutex;
  mutable std::condition_variable _changed;
  state _state = state::open;
  std::optional<stage_load> _sum;
  std::exception_ptr _error;
};

/**
 * @brief Whether a join's build goes whole to every rank, chosen alike on every rank from what
 * the ranks really hold.
 *
 * The build side's exchange sinks hold their input and count it. A build of at most the limit
 * over all ranks, which the join then finds worth it (weigh()), goes whole to every rank: the
 * probe rows then stay where they are, and each rank's filters hold every key. Any other build
 * is hashed, as planned. A rank holds at most its share, the limit split evenly: one that passes
 * it calls over(), which settles a hashed build without waiting for the other ranks.
 */
class broadcast_choice {
 public:
  broadcast_choice(std::shared_ptr<exchange_service> service,
                   stream_id stream,
                   std::uint64_t limit_bytes,
                   cucascade::memory::memory_space& gpu,
                   std::chrono::milliseconds timeout = stage_count::default_timeout);

  /// What the join makes of a build within the limit, from all ranks' bytes and rows: whether to
  /// send it whole. The same function of the same sums on every rank. While planning only.
  void weigh(std::function<bool(stage_load const&)> worth) { _worth = std::move(worth); }

  /// The bytes one rank may hold before it must call over().
  [[nodiscard]] std::uint64_t share() const noexcept { return _share; }

  /// This rank holds more than its share: the build is hashed. Does not wait.
  void over() { _count.over(); }

  /// This rank's input ended with @p held held. Blocks as stage_count::total().
  /// @return Whether the build goes whole to every rank.
  [[nodiscard]] bool decide(stage_load held, ::cuda::stream_ref stream)
  {
    return settled(_count.total(held, stream));
  }

  /// The choice, once this rank has it; nothing before. @throws as stage_count::total().
  [[nodiscard]] std::optional<bool> known() const;

  /// The choice; blocks until this rank has it. @throws as stage_count::wait().
  [[nodiscard]] bool wait() const { return settled(_count.wait()); }

  /// The GPU the counts are gathered on.
  [[nodiscard]] cucascade::memory::memory_space& gpu() const noexcept { return _count.gpu(); }

 private:
  /// The choice for @p sum, made once.
  [[nodiscard]] bool settled(std::optional<stage_load> const& sum) const;

  stage_count _count;
  std::uint64_t _limit;
  std::uint64_t _share;
  std::function<bool(stage_load const&)> _worth;
  mutable std::once_flag _weighed;
  mutable bool _whole = false;
};

}  // namespace sirius::exchange
