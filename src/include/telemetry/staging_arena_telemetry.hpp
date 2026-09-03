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

#include "telemetry-bridge/gen/channel.rs.h"
#include "telemetry-bridge/gen/data_batch.rs.h"
#include "telemetry-bridge/gen/memory.rs.h"
#include "telemetry-bridge/gen/uuid.rs.h"

#include <cucascade/memory/common.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace cucascade::memory {
class memory_reservation_manager;
}  // namespace cucascade::memory

namespace sirius::telemetry {

class telemetry_context;

/// Quent view of the cross-node exchange staging arena (exec::exchange_staging_arena).
///
/// The arena is one cudaMalloc/fabric slab outside every cucascade memory space, so
/// memory_context never declares a Memory for it and bytes that land there are invisible.
/// This class declares a stdlib Memory `exchange-staging-arena` under the engine, one
/// Channel to and from every memory space the manager knows, and reports each lease as a
/// DataBatch (`instance_name = "staging_lease"`, no producing pipeline):
///
///   lease          constructed => stationary(arena, aligned lease length)
///   export_packed  stationary => in_transit(source space -> arena, packed bytes) => stationary
///   push_packed    stationary => in_transit(arena -> dest space, packed bytes)   => stationary
///   release        stationary => destructed, exit
///
/// The Stationary usage is always the booked lease length, so the sum of live Stationary
/// usages on the arena Memory is its occupancy; the transfer size rides the InTransit channel
/// usage. Every method is thread-safe and never throws: leases arrive from RPC and transport
/// threads, and a telemetry failure must not fail an exchange.
class staging_arena_telemetry {
 public:
  /// `context` must be non-null. `manager` may be null (no channels are declared, transfers
  /// then leave the lease stationary).
  staging_arena_telemetry(std::shared_ptr<const telemetry_context> context,
                          const cucascade::memory::memory_reservation_manager* manager,
                          uint64_t capacity_bytes);
  ~staging_arena_telemetry();

  staging_arena_telemetry(const staging_arena_telemetry&)            = delete;
  staging_arena_telemetry& operator=(const staging_arena_telemetry&) = delete;
  staging_arena_telemetry(staging_arena_telemetry&&)                 = delete;
  staging_arena_telemetry& operator=(staging_arena_telemetry&&)      = delete;

  /// `bytes` is the aligned length the arena booked for the lease at `offset`.
  void on_lease(uint64_t offset, uint64_t bytes) noexcept;
  /// A lease released while in transit is settled (stationary) first, then destructed.
  void on_release(uint64_t offset) noexcept;

  /// export_packed gathers `bytes` from a batch resident in `source` into the lease.
  void on_pack_started(uint64_t offset,
                       cucascade::memory::memory_space_id source,
                       uint64_t bytes) noexcept;
  void on_pack_completed(uint64_t offset) noexcept;

  /// push_packed deep-copies `bytes` out of the lease into pool memory in `dest`.
  void on_unpack_started(uint64_t offset,
                         cucascade::memory::memory_space_id dest,
                         uint64_t bytes) noexcept;
  void on_unpack_completed(uint64_t offset) noexcept;

  [[nodiscard]] uuid::UUID memory_id() const noexcept;
  /// Leases currently reported live. Diagnostics and tests.
  [[nodiscard]] std::size_t live_leases() const;

 private:
  using space_id = cucascade::memory::memory_space_id;

  struct lease {
    rust::Box<quent::data_batch::DataBatchHandle> handle;
    uint64_t bytes;
    bool in_transit;
  };

  void start_transit(uint64_t offset,
                     const space_id& space,
                     bool to_arena,
                     uint64_t bytes) noexcept;
  void settle(uint64_t offset) noexcept;
  /// Back to stationary(arena, lease bytes) if in transit; `mutex_` must be held.
  void settle_locked(lease& l) noexcept;

  std::shared_ptr<const telemetry_context> context_;
  rust::Box<quent::memory::MemoryHandle> memory_;
  std::unordered_map<space_id, rust::Box<quent::channel::ChannelHandle>> to_arena_;
  std::unordered_map<space_id, rust::Box<quent::channel::ChannelHandle>> from_arena_;
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, lease> leases_;
};

}  // namespace sirius::telemetry
