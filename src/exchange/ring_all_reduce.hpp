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

#include <rmm/cuda_stream_view.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>

namespace cucascade::memory {
class memory_space;
}

namespace sirius::exchange {

class exchange_service;

/// Folds @p count words of @p from into @p into on @p stream: the reduction of ring_all_reduce.
using ring_combine = std::function<void(
  std::uint32_t* into, std::uint32_t const* from, std::size_t count, rmm::cuda_stream_view stream)>;

/**
 * @brief Reduces every rank's @p words in place, so each rank ends with combine() of all of them.
 *
 * A reduce-scatter then an all-gather around the ring of ranks: the words split into world-size
 * slices, and in each of 2 x (world - 1) steps a rank sends one slice to the next rank and takes
 * one from the previous. Each rank sends about 2 x @p count words whatever the world size, and
 * holds one slice of another rank's at a time, against world - 1 whole copies for an all-gather.
 *
 * Every rank calls it with the same stream id, the same @p count and the same combine(). Each step
 * waits for the previous rank, so one rank missing stalls the whole ring: a rank that leaves or
 * fails closes its side, and the next rank then fails at once, and so on around the ring. Never
 * call it on the transport's engine thread.
 *
 * @param words This rank's words, on the GPU; overwritten with the reduction.
 * @param gpu Where slices that landed elsewhere are brought, and where sent slices are copied.
 * @param timeout How long one step may wait for the previous rank.
 * @throws sirius::internal_exception when a step times out or the previous rank left the ring;
 * sirius::invalid_input_exception when @p count is less than the world size.
 */
void ring_all_reduce(exchange_service& service,
                     stream_id stream,
                     std::uint32_t* words,
                     std::size_t count,
                     ring_combine const& combine,
                     cucascade::memory::memory_space& gpu,
                     rmm::cuda_stream_view cuda_stream,
                     std::chrono::milliseconds timeout = std::chrono::minutes{10});

/// Takes this rank's part in the ring on @p stream without sending anything: the next rank then
/// fails its ring at once instead of at the timeout. Does not wait.
void leave_ring(exchange_service& service, stream_id stream);

}  // namespace sirius::exchange
