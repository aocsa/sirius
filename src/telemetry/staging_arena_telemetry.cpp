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

#include "telemetry/staging_arena_telemetry.hpp"

#include "data/data_batch_utils.hpp"  // sirius::get_next_batch_id
#include "log/logging.hpp"
#include "telemetry/memory_context.hpp"
#include "telemetry/telemetry_context.hpp"

#include <cucascade/memory/memory_reservation_manager.hpp>
#include <cucascade/memory/memory_space.hpp>

#include <format>
#include <limits>
#include <string>

namespace sirius::telemetry {

namespace {

constexpr const char* kArenaName = "exchange-staging-arena";

std::string tier_name(cucascade::memory::Tier tier)
{
  switch (tier) {
    case cucascade::memory::Tier::GPU: return "gpu";
    case cucascade::memory::Tier::HOST: return "host";
    case cucascade::memory::Tier::DISK: return "disk";
    default: return "unknown";
  }
}

}  // namespace

staging_arena_telemetry::staging_arena_telemetry(
  std::shared_ptr<const telemetry_context> context,
  const cucascade::memory::memory_reservation_manager* manager,
  uint64_t capacity_bytes)
  : context_(std::move(context)),
    memory_(quent::memory::create(context_->context(),
                                  {
                                    .instance_name   = kArenaName,
                                    .parent_group_id = context_->engine_id(),
                                  }))
{
  memory_->operating({.capacity_bytes = capacity_bytes});
  if (manager == nullptr) { return; }

  // Same naming as memory_context's channels ("gpu-0->host-0"), so the arena reads as one more
  // endpoint of the existing memory graph.
  const auto& memory_context = context_->get_memory_context();
  for (const auto* space : manager->get_all_memory_spaces()) {
    if (space == nullptr) { continue; }
    const auto id     = space->get_id();
    auto space_handle = memory_context->get_memory_handle(id);
    if (!space_handle) { continue; }
    const auto space_uuid = space_handle->get().uuid();
    const auto space_name = std::format("{}-{}", tier_name(id.tier), id.device_id);

    auto to_arena =
      quent::channel::create(context_->context(),
                             {
                               .instance_name   = std::format("{}->{}", space_name, kArenaName),
                               .parent_group_id = context_->engine_id(),
                               .source_id       = space_uuid,
                               .target_id       = memory_->uuid(),
                             });
    to_arena->operating({.capacity_bytes = std::numeric_limits<uint64_t>::max()});
    to_arena_.emplace(id, std::move(to_arena));

    auto from_arena =
      quent::channel::create(context_->context(),
                             {
                               .instance_name   = std::format("{}->{}", kArenaName, space_name),
                               .parent_group_id = context_->engine_id(),
                               .source_id       = memory_->uuid(),
                               .target_id       = space_uuid,
                             });
    from_arena->operating({.capacity_bytes = std::numeric_limits<uint64_t>::max()});
    from_arena_.emplace(id, std::move(from_arena));
  }
}

staging_arena_telemetry::~staging_arena_telemetry()
{
  {
    // Leases still live here were leaked by their owner; close them so the arena Memory's
    // occupancy returns to zero in the trace instead of dangling past the engine's exit.
    const std::lock_guard lock(mutex_);
    for (auto& [offset, l] : leases_) {
      settle_locked(l);
      l.handle->destructed();
      l.handle->exit();
    }
    leases_.clear();
  }
  for (auto& [_, channel] : to_arena_) {
    channel->finalizing();
    channel->exit();
  }
  for (auto& [_, channel] : from_arena_) {
    channel->finalizing();
    channel->exit();
  }
  memory_->finalizing();
  memory_->exit();
}

void staging_arena_telemetry::on_lease(uint64_t offset, uint64_t bytes) noexcept
{
  try {
    // The id comes from the process-wide batch counter so it never collides with a real batch's.
    auto handle = quent::data_batch::create(context_->context(),
                                            {
                                              .instance_name          = "staging_lease",
                                              .data_batch_id          = sirius::get_next_batch_id(),
                                              .producer_pipeline_uuid = uuid::new_nil(),
                                            });
    handle->stationary({
      .memory_resource_id    = memory_->uuid(),
      .memory_capacity_bytes = bytes,
    });
    const std::lock_guard lock(mutex_);
    if (auto it = leases_.find(offset); it != leases_.end()) {
      // The arena never hands out a live offset twice, so a stale entry means a release went
      // unreported; close it rather than leak it.
      SIRIUS_LOG_WARN("staging arena telemetry: lease at offset {} re-issued while still live",
                      offset);
      settle_locked(it->second);
      it->second.handle->destructed();
      it->second.handle->exit();
      leases_.erase(it);
    }
    leases_.emplace(offset,
                    lease{.handle = std::move(handle), .bytes = bytes, .in_transit = false});
  } catch (...) {
  }
}

void staging_arena_telemetry::on_release(uint64_t offset) noexcept
{
  try {
    const std::lock_guard lock(mutex_);
    auto it = leases_.find(offset);
    if (it == leases_.end()) { return; }
    settle_locked(it->second);
    it->second.handle->destructed();
    it->second.handle->exit();
    leases_.erase(it);
  } catch (...) {
  }
}

void staging_arena_telemetry::on_pack_started(uint64_t offset,
                                              cucascade::memory::memory_space_id source,
                                              uint64_t bytes) noexcept
{
  start_transit(offset, source, /*to_arena=*/true, bytes);
}

void staging_arena_telemetry::on_pack_completed(uint64_t offset) noexcept { settle(offset); }

void staging_arena_telemetry::on_unpack_started(uint64_t offset,
                                                cucascade::memory::memory_space_id dest,
                                                uint64_t bytes) noexcept
{
  start_transit(offset, dest, /*to_arena=*/false, bytes);
}

void staging_arena_telemetry::on_unpack_completed(uint64_t offset) noexcept { settle(offset); }

uuid::UUID staging_arena_telemetry::memory_id() const noexcept { return memory_->uuid(); }

std::size_t staging_arena_telemetry::live_leases() const
{
  const std::lock_guard lock(mutex_);
  return leases_.size();
}

void staging_arena_telemetry::start_transit(uint64_t offset,
                                            const space_id& space,
                                            bool to_arena,
                                            uint64_t bytes) noexcept
{
  try {
    const std::lock_guard lock(mutex_);
    auto it = leases_.find(offset);
    if (it == leases_.end()) { return; }
    auto& l = it->second;

    auto space_handle    = context_->get_memory_context()->get_memory_handle(space);
    const auto& channels = to_arena ? to_arena_ : from_arena_;
    auto channel         = channels.find(space);
    if (!space_handle || channel == channels.end()) {
      SIRIUS_LOG_DEBUG(
        "staging arena telemetry: no Memory/Channel for {}-{}; lease at {} stays stationary",
        tier_name(space.tier),
        space.device_id,
        offset);
      return;
    }

    settle_locked(l);  // in_transit => in_transit is not a DataBatch transition
    const auto space_uuid = space_handle->get().uuid();
    l.handle->in_transit({
      .source_memory_resource_id    = to_arena ? space_uuid : memory_->uuid(),
      .source_memory_capacity_bytes = bytes,
      .dest_memory_resource_id      = to_arena ? memory_->uuid() : space_uuid,
      .dest_memory_capacity_bytes   = bytes,
      .channel_resource_id          = channel->second->uuid(),
      .channel_capacity_bytes       = bytes,
    });
    l.in_transit = true;
  } catch (...) {
  }
}

void staging_arena_telemetry::settle(uint64_t offset) noexcept
{
  try {
    const std::lock_guard lock(mutex_);
    if (auto it = leases_.find(offset); it != leases_.end()) { settle_locked(it->second); }
  } catch (...) {
  }
}

void staging_arena_telemetry::settle_locked(lease& l) noexcept
{
  if (!l.in_transit) { return; }
  l.handle->stationary({
    .memory_resource_id    = memory_->uuid(),
    .memory_capacity_bytes = l.bytes,
  });
  l.in_transit = false;
}

}  // namespace sirius::telemetry
