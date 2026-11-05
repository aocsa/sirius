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

#include "exchange/ring_all_reduce.hpp"

#include "data/data_batch_utils.hpp"
#include "data/sirius_converter_registry.hpp"
#include "exchange/exchange_service.hpp"
#include "sirius/exception.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/table/table.hpp>
#include <cudf/utilities/error.hpp>

#include <cucascade/cudf/gpu_data_representation.hpp>
#include <cucascade/data/data_batch.hpp>
#include <cucascade/memory/memory_space.hpp>

#include <condition_variable>
#include <map>
#include <mutex>

namespace sirius::exchange {
namespace {

/// The previous rank's slices, by step, and its end.
struct ring_inbox : inbound {
  rank_t const from;
  stream_id const stream;
  std::mutex mutex;
  std::condition_variable changed;
  std::map<std::uint64_t, std::shared_ptr<cucascade::data_batch>> slices;
  bool ended = false;
  std::exception_ptr error;

  ring_inbox(rank_t previous, stream_id id) : from(previous), stream(id) {}

  void on_batch(rank_t sender,
                std::uint64_t step,
                std::shared_ptr<cucascade::data_batch> data) override
  {
    std::lock_guard lock{mutex};
    if (sender != from) {
      error = std::make_exception_ptr(sirius::internal_exception(
        "exchange ring on stream {}: a slice from rank {}, not rank {}", stream, sender, from));
    } else {
      slices.emplace(step, std::move(data));
    }
    changed.notify_all();
  }
  void on_end(rank_t sender) override
  {
    std::lock_guard lock{mutex};
    if (sender == from) { ended = true; }
    changed.notify_all();
  }
  void on_error(std::exception_ptr e) override
  {
    std::lock_guard lock{mutex};
    error = std::move(e);
    changed.notify_all();
  }

  /// Step @p step's slice.
  std::shared_ptr<cucascade::data_batch> take(std::uint64_t step,
                                              std::chrono::steady_clock::time_point deadline)
  {
    std::unique_lock lock{mutex};
    if (!changed.wait_until(
          lock, deadline, [&] { return error || ended || slices.contains(step); })) {
      throw sirius::internal_exception(
        "exchange ring on stream {}: no slice from rank {} at step {}", stream, from, step);
    }
    if (error) { std::rethrow_exception(error); }
    auto const it = slices.find(step);
    if (it == slices.end()) {
      throw sirius::internal_exception(
        "exchange ring on stream {}: rank {} left before step {}", stream, from, step);
    }
    auto slice = std::move(it->second);
    slices.erase(it);
    return slice;
  }
};

}  // namespace

void ring_all_reduce(exchange_service& service,
                     stream_id stream,
                     std::uint32_t* words,
                     std::size_t count,
                     ring_combine const& combine,
                     cucascade::memory::memory_space& gpu,
                     rmm::cuda_stream_view cuda_stream,
                     std::chrono::milliseconds timeout)
{
  auto const world = service.world_size();
  if (world == 1) { return; }
  if (count < world) {
    throw sirius::invalid_input_exception(
      "exchange ring: {} words cannot be split among {} ranks", count, world);
  }
  auto const rank = service.rank();
  auto const next = (rank + 1) % world;
  auto const prev = (rank + world - 1) % world;
  auto inbox      = std::make_shared<ring_inbox>(prev, stream);
  service.attach(stream, inbox);
  struct detach_on_exit {
    exchange_service& s;
    stream_id id;
    ~detach_on_exit() { s.detach(id); }
  } const detach{service, stream};

  auto const begin = [&](std::size_t slice) { return count * slice / world; };
  auto const end   = [&](std::size_t slice) { return count * (slice + 1) / world; };
  // Slice indices modulo the world size, from a step that may run below zero.
  auto const slice_at = [&](std::int64_t i) {
    auto const w = static_cast<std::int64_t>(world);
    return static_cast<std::size_t>(((i % w) + w) % w);
  };
  auto const mr = gpu.get_default_allocator();
  bool closed   = false;
  try {
    auto const steps = 2 * (std::uint64_t{world} - 1);
    for (std::uint64_t step = 0; step < steps; ++step) {
      // Reduce-scatter, then all-gather: after world - 1 steps a rank holds the reduction of slice
      // rank + 1, and passes the reductions on around the ring.
      bool const reducing = step < world - 1;
      auto const s        = static_cast<std::int64_t>(reducing ? step : step - (world - 1));
      auto const r        = static_cast<std::int64_t>(rank);
      auto const sending  = reducing ? slice_at(r - s) : slice_at(r + 1 - s);
      auto const taking   = reducing ? slice_at(r - s - 1) : slice_at(r - s);

      auto const sent_words = end(sending) - begin(sending);
      auto column           = cudf::make_numeric_column(cudf::data_type{cudf::type_id::UINT32},
                                              static_cast<cudf::size_type>(sent_words),
                                              cudf::mask_state::UNALLOCATED,
                                              cuda_stream,
                                              mr);
      CUDF_CUDA_TRY(cudaMemcpyAsync(column->mutable_view().data<std::uint32_t>(),
                                    words + begin(sending),
                                    sent_words * sizeof(std::uint32_t),
                                    cudaMemcpyDeviceToDevice,
                                    cuda_stream.value()));
      std::vector<std::unique_ptr<cudf::column>> columns;
      columns.push_back(std::move(column));
      service.send(stream,
                   next,
                   make_data_batch(std::make_unique<cudf::table>(std::move(columns)),
                                   gpu,
                                   cuda_stream,
                                   telemetry::batch_telemetry_info{}));

      auto slice = inbox->take(step, std::chrono::steady_clock::now() + timeout);
      auto ro    = slice->to_read_only();
      if (ro.get_current_tier() != cucascade::memory::Tier::GPU) {
        slice = ro.clone_to<cucascade::gpu_table_representation>(
          sirius::converter_registry::get(), sirius::get_next_batch_id(), &gpu, cuda_stream);
        ro = slice->to_read_only();
      }
      if (cudaEvent_t writer = ro.get_writer_event()) {
        CUDF_CUDA_TRY(cudaStreamWaitEvent(cuda_stream.value(), writer, 0));
      }
      auto const theirs      = get_cudf_table_view(ro).column(0);
      auto const taken_words = end(taking) - begin(taking);
      if (static_cast<std::size_t>(theirs.size()) != taken_words) {
        throw sirius::internal_exception(
          "exchange ring on stream {}: step {} brought {} words, expected {}",
          stream,
          step,
          theirs.size(),
          taken_words);
      }
      if (reducing) {
        combine(words + begin(taking), theirs.data<std::uint32_t>(), taken_words, cuda_stream);
      } else {
        CUDF_CUDA_TRY(cudaMemcpyAsync(words + begin(taking),
                                      theirs.data<std::uint32_t>(),
                                      taken_words * sizeof(std::uint32_t),
                                      cudaMemcpyDeviceToDevice,
                                      cuda_stream.value()));
      }
      // The slice is freed when the next step takes its place.
      cuda_stream.synchronize();
    }
    // The previous rank's end may still be on its way; once detached, the stream drops it.
    closed = true;
    service.close(stream, next);
  } catch (...) {
    // The next rank waits on this one: tell it now, rather than at its timeout.
    if (!closed) {
      try {
        service.close(stream, next);
      } catch (...) {
      }
    }
    throw;
  }
}

void leave_ring(exchange_service& service, stream_id stream)
{
  auto const world = service.world_size();
  if (world == 1) { return; }
  service.detach(stream);
  service.close(stream, (service.rank() + 1) % world);
}

}  // namespace sirius::exchange
