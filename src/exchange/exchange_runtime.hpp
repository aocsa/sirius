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

#include "exchange/host_staging.hpp"
#include "exchange/transport.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace cucascade::memory {
class memory_space;
}

namespace sirius::exchange {

class exchange_service;

/// The most TCP lanes a rank may have.
constexpr std::uint32_t max_tcp_lanes = 64;
/// The most receive frames per peer.
constexpr std::uint32_t max_recv_frames_per_peer = 256;

/// "auto", "always", "never" or "remote", as the sirius_exchange_host_staging setting takes it.
/// @throws sirius::invalid_input_exception on any other value.
[[nodiscard]] host_staging parse_host_staging(std::string_view value);

/// Ring sizing: @p send_frames staging frames shared by all peers, and @p recv_frames_per_peer
/// frames (credits) for each peer, each @p frame_bytes, a positive multiple of 256. 8 MiB frames
/// keep NIXL writes large enough to reach link bandwidth. The sizing is fixed until credits are
/// granted on demand (#2371).
struct nixl_exchange_options {
  std::size_t frame_bytes   = std::size_t{8} * 1024 * 1024;  // 8 MiB
  std::uint32_t send_frames = 16;
  /// The frames a peer may have in flight to this rank. Over TCP lanes a peer keeps one frame per
  /// lane in flight, so fewer frames than lanes leave lanes idle.
  std::uint32_t recv_frames_per_peer = 8;
  host_staging staging               = host_staging::automatic;
  /// TCP lanes per rank (at most max_tcp_lanes); 0 shares 16 among the ranks on this host.
  std::uint32_t tcp_lanes = 0;
  transport::options transport_options;
};

/**
 * @brief This process's exchange over NIXL: its rings, its fabric, connected to every peer, and
 * the service on top.
 *
 * Blocks until every peer is reached. The service shares ownership of the rings and the fabric,
 * so they outlive every plan that still holds it.
 *
 * @param peers "host:port" of every rank's listener, indexed by rank.
 * @throws sirius::internal_exception when NIXL fails or a peer is not reached within two minutes.
 */
[[nodiscard]] std::shared_ptr<exchange_service> make_nixl_exchange(
  rank_t rank,
  std::vector<std::string> peers,
  cucascade::memory::memory_space& gpu,
  cucascade::memory::memory_space* host,
  nixl_exchange_options opts = {});

}  // namespace sirius::exchange
