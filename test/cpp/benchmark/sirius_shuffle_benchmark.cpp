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

#include "data/data_batch_utils.hpp"
#include "data/sirius_converter_registry.hpp"
#include "memory/sirius_memory_reservation_manager.hpp"
#include "op/partition/gpu_partition_impl.hpp"
#include "pipeline/batch_lock_utils.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>

#include <rmm/cuda_device.hpp>
#include <rmm/cuda_stream.hpp>
#include <rmm/cuda_stream_view.hpp>

#include <cuda_runtime_api.h>
#include <nvtx3/nvtx3.hpp>

#include <cucascade/cudf/gpu_data_representation.hpp>
#include <cucascade/data/data_batch.hpp>
#include <cucascade/memory/common.hpp>
#include <cucascade/memory/memory_space.hpp>
#include <cucascade/memory/reservation_manager_configurator.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;

constexpr uint64_t KiB = 1024ULL;
constexpr uint64_t MiB = 1024ULL * KiB;
constexpr uint64_t GiB = 1024ULL * MiB;

void check_cuda(cudaError_t err, char const* what)
{
  if (err != cudaSuccess) {
    throw std::runtime_error(std::string(what) + ": " + cudaGetErrorString(err));
  }
}

double median_of(std::vector<double> values)
{
  if (values.empty()) { return 0.0; }
  std::sort(values.begin(), values.end());
  auto const n = values.size();
  if (n % 2 == 1) { return values[n / 2]; }
  return 0.5 * (values[n / 2 - 1] + values[n / 2]);
}

std::uint64_t parse_size_bytes(std::string spec)
{
  while (!spec.empty() && spec.front() == ' ') {
    spec.erase(spec.begin());
  }
  while (!spec.empty() && spec.back() == ' ') {
    spec.pop_back();
  }
  if (spec.empty()) { throw std::invalid_argument("empty size"); }

  std::size_t idx  = 0;
  double value     = std::stod(spec, &idx);
  std::string unit = spec.substr(idx);
  for (char& c : unit) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  std::uint64_t mul = 1;
  if (unit.empty() || unit == "B") {
    mul = 1;
  } else if (unit == "K" || unit == "KB" || unit == "KIB") {
    mul = KiB;
  } else if (unit == "M" || unit == "MB" || unit == "MIB") {
    mul = MiB;
  } else if (unit == "G" || unit == "GB" || unit == "GIB") {
    mul = GiB;
  } else {
    throw std::invalid_argument("unrecognized size unit in '" + spec + "'");
  }
  if (value <= 0) { throw std::invalid_argument("size must be positive"); }
  return static_cast<std::uint64_t>(value * static_cast<double>(mul));
}

std::vector<std::uint64_t> parse_size_list(std::string const& spec)
{
  std::vector<std::uint64_t> out;
  std::stringstream ss(spec);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item.empty()) { continue; }
    out.push_back(parse_size_bytes(item));
  }
  if (out.empty()) { throw std::invalid_argument("no sizes given"); }
  return out;
}

int query_pcie_gen(int device)
{
  std::string cmd = "nvidia-smi -i " + std::to_string(device) +
                    " --query-gpu=pcie.link.gen.current --format=csv,noheader,nounits";
  FILE* pipe = popen(cmd.c_str(), "r");
  if (pipe == nullptr) { return -1; }
  char buf[32] = {};
  if (std::fgets(buf, sizeof(buf), pipe) == nullptr) {
    pclose(pipe);
    return -1;
  }
  pclose(pipe);
  try {
    return std::stoi(buf);
  } catch (...) {
    return -1;
  }
}

bool enable_p2p(int num_gpus)
{
  bool all_enabled = true;
  for (int i = 0; i < num_gpus; ++i) {
    for (int j = 0; j < num_gpus; ++j) {
      if (i == j) { continue; }
      int can_access = 0;
      if (cudaDeviceCanAccessPeer(&can_access, i, j) != cudaSuccess || !can_access) {
        (void)cudaGetLastError();
        all_enabled = false;
        continue;
      }
      check_cuda(cudaSetDevice(i), "cudaSetDevice");
      cudaError_t enable_err = cudaDeviceEnablePeerAccess(j, 0);
      (void)cudaGetLastError();
      if (enable_err != cudaSuccess && enable_err != cudaErrorPeerAccessAlreadyEnabled) {
        all_enabled = false;
      }
    }
  }
  check_cuda(cudaSetDevice(0), "cudaSetDevice(0)");
  (void)cudaGetLastError();
  return all_enabled;
}

std::size_t batch_bytes(cucascade::data_batch& batch)
{
  auto ro = batch.to_read_only();
  if (ro.get_data() == nullptr) { return 0; }
  return ro.get_data()->get_size_in_bytes();
}

std::size_t batch_rows(cucascade::data_batch& batch)
{
  return static_cast<std::size_t>(sirius::get_cudf_table_view(batch).num_rows());
}

std::string copy_path_label(bool peer_01, bool peer_10)
{
  if (peer_01 && peer_10) { return "peer_dma"; }
  if (!peer_01 && !peer_10) { return "host_staging"; }
  return "mixed";
}

struct Cli {
  std::string mode{"all"};
  std::string sizes{"1MiB,16MiB,256MiB,1GiB"};
  int reps         = 8;
  int warmup       = 2;
  int payload_cols = 1;
  std::string csv_path;
};

void print_usage(std::ostream& out)
{
  out << "sirius_shuffle_benchmark — two-GPU Sirius primitive shuffle microbench\n"
      << "\n"
      << "Usage: sirius_shuffle_benchmark [options]\n"
      << "  --mode MODE          transfer | partition | shuffle | all  (default: all)\n"
      << "  --sizes LIST         comma-separated sizes, e.g. 1MiB,16MiB,256MiB,1GiB\n"
      << "  --reps N             timed repetitions per size/mode (default: 8)\n"
      << "  --warmup N           untimed warmup reps (default: 2)\n"
      << "  --payload-cols N     INT64 payload columns besides the key (default: 1)\n"
      << "  --csv PATH           write CSV to PATH (also printed to stdout)\n"
      << "  -h, --help           this message\n"
      << "\n"
      << "Requires >=2 CUDA devices. Exits 0 with a message if fewer are visible.\n"
      << "hash_partition keeps slices on the source GPU; cross-GPU movement is\n"
      << "lock_or_prepare_batch -> clone_to -> convert_gpu_to_gpu.\n";
}

Cli parse_cli(int argc, char** argv)
{
  Cli cli;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto need       = [&](char const* name) -> std::string {
      if (i + 1 >= argc) { throw std::invalid_argument(std::string(name) + " requires a value"); }
      return argv[++i];
    };
    if (arg == "-h" || arg == "--help") {
      print_usage(std::cout);
      std::exit(0);
    } else if (arg == "--mode") {
      cli.mode = need("--mode");
    } else if (arg == "--sizes") {
      cli.sizes = need("--sizes");
    } else if (arg == "--reps") {
      cli.reps = std::stoi(need("--reps"));
    } else if (arg == "--warmup") {
      cli.warmup = std::stoi(need("--warmup"));
    } else if (arg == "--payload-cols") {
      cli.payload_cols = std::stoi(need("--payload-cols"));
    } else if (arg == "--csv") {
      cli.csv_path = need("--csv");
    } else {
      throw std::invalid_argument("unknown argument: " + arg);
    }
  }
  if (cli.reps < 1) { throw std::invalid_argument("--reps must be >= 1"); }
  if (cli.warmup < 0) { throw std::invalid_argument("--warmup must be >= 0"); }
  if (cli.payload_cols < 1) { throw std::invalid_argument("--payload-cols must be >= 1"); }
  return cli;
}

struct Fixture {
  std::unique_ptr<sirius::memory::sirius_memory_reservation_manager> manager;
  cucascade::memory::memory_space* gpu0 = nullptr;
  cucascade::memory::memory_space* gpu1 = nullptr;
  std::unique_ptr<rmm::cuda_stream> stream0;
  std::unique_ptr<rmm::cuda_stream> stream1;
  bool peer_01 = false;
  bool peer_10 = false;
  int pcie_gen = -1;

  void setup()
  {
    cucascade::memory::reservation_manager_configurator builder;
    builder.set_number_of_gpus(2)
      .set_gpu_usage_limit(16ULL * GiB)
      .set_reservation_fraction_per_gpu(0.75)
      .set_per_numa_region_capacity(8ULL * GiB)
      .use_numa_id_as_host_id()
      .track_reservation_per_stream(false)
      .set_reservation_fraction_per_numa_region(0.75);
    manager = std::make_unique<sirius::memory::sirius_memory_reservation_manager>(builder.build());
    sirius::converter_registry::initialize();

    auto gpu_spaces = manager->get_memory_spaces_for_tier(cucascade::memory::Tier::GPU);
    if (gpu_spaces.size() < 2) { throw std::runtime_error("expected two GPU memory spaces"); }
    gpu0 = const_cast<cucascade::memory::memory_space*>(gpu_spaces[0]);
    gpu1 = const_cast<cucascade::memory::memory_space*>(gpu_spaces[1]);

    enable_p2p(2);
    peer_01 = cucascade::memory::probe_peer_dma_works(gpu0->get_device_id(), gpu1->get_device_id());
    peer_10 = cucascade::memory::probe_peer_dma_works(gpu1->get_device_id(), gpu0->get_device_id());

    {
      rmm::cuda_set_device_raii guard{rmm::cuda_device_id{gpu0->get_device_id()}};
      stream0 = std::make_unique<rmm::cuda_stream>();
    }
    {
      rmm::cuda_set_device_raii guard{rmm::cuda_device_id{gpu1->get_device_id()}};
      stream1 = std::make_unique<rmm::cuda_stream>();
    }
    pcie_gen = query_pcie_gen(gpu0->get_device_id());
  }

  ~Fixture()
  {
    stream0.reset();
    stream1.reset();
    if (manager) {
      manager->shutdown();
      sirius::converter_registry::shutdown();
    }
  }
};

std::shared_ptr<cucascade::data_batch> make_uniform_batch(cucascade::memory::memory_space& space,
                                                          rmm::cuda_stream_view stream,
                                                          std::size_t num_rows,
                                                          int payload_cols,
                                                          std::uint64_t seed)
{
  rmm::cuda_set_device_raii guard{rmm::cuda_device_id{space.get_device_id()}};
  auto mr = space.get_default_allocator();

  std::vector<std::unique_ptr<cudf::column>> cols;
  cols.reserve(static_cast<std::size_t>(payload_cols) + 1);

  auto key = cudf::make_numeric_column(cudf::data_type{cudf::type_id::INT64},
                                       static_cast<cudf::size_type>(num_rows),
                                       cudf::mask_state::UNALLOCATED,
                                       stream,
                                       mr);
  std::vector<std::int64_t> host_keys(num_rows);
  // SplitMix64 so keys are uniform and independent of which GPU they live on.
  std::uint64_t state = seed;
  for (std::size_t i = 0; i < num_rows; ++i) {
    state += 0x9E3779B97F4A7C15ULL;
    std::uint64_t z = state;
    z               = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z               = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    host_keys[i]    = static_cast<std::int64_t>(z ^ (z >> 31));
  }
  check_cuda(cudaMemcpyAsync(key->mutable_view().data<std::int64_t>(),
                             host_keys.data(),
                             host_keys.size() * sizeof(std::int64_t),
                             cudaMemcpyHostToDevice,
                             stream.value()),
             "cudaMemcpyAsync keys");
  cols.push_back(std::move(key));

  for (int c = 0; c < payload_cols; ++c) {
    cudf::numeric_scalar<std::int64_t> start(
      static_cast<std::int64_t>(c) * 1'000'000, true, stream);
    cudf::numeric_scalar<std::int64_t> step(1, true, stream);
    cols.push_back(cudf::sequence(static_cast<cudf::size_type>(num_rows), start, step, stream, mr));
  }
  stream.synchronize();
  cudf::table table(std::move(cols));
  return sirius::make_data_batch(
    std::move(table), space, stream, sirius::telemetry::batch_telemetry_info{});
}

std::shared_ptr<cucascade::data_batch> clone_to_gpu(
  std::shared_ptr<cucascade::data_batch> const& src,
  cucascade::memory::memory_space* dst_space,
  rmm::cuda_stream_view stream)
{
  rmm::cuda_set_device_raii guard{rmm::cuda_device_id{dst_space->get_device_id()}};
  auto prepared = sirius::pipeline::lock_or_prepare_batch(src, dst_space, stream);
  if (!prepared) { throw std::runtime_error("lock_or_prepare_batch failed"); }
  return cucascade::data_batch::to_idle(std::move(*prepared));
}

struct CsvRow {
  std::uint64_t size_bytes = 0;
  std::string mode;
  std::string copy_path;
  double partition_ms         = 0;
  double transfer_ms          = 0;
  double shuffle_ms           = 0;
  double remote_bytes_0to1    = 0;
  double remote_bytes_1to0    = 0;
  double imbalance            = 0;
  double partition_rows_per_s = 0;
  double effective_GBps       = 0;
  double transfer_GBps        = 0;
  int pcie_gen                = -1;
  double physical_copy_bytes  = 0;
};

void write_csv_header(std::ostream& out)
{
  out << "size_bytes,mode,copy_path,partition_ms,transfer_ms,shuffle_ms,"
         "remote_bytes_0to1,remote_bytes_1to0,imbalance,partition_rows_per_s,"
         "effective_GBps,transfer_GBps,pcie_gen,physical_copy_bytes\n";
}

void write_csv_row(std::ostream& out, CsvRow const& r)
{
  out << r.size_bytes << ',' << r.mode << ',' << r.copy_path << ',' << std::fixed
      << std::setprecision(4) << r.partition_ms << ',' << r.transfer_ms << ',' << r.shuffle_ms
      << ',' << std::setprecision(0) << r.remote_bytes_0to1 << ',' << r.remote_bytes_1to0 << ','
      << std::setprecision(4) << r.imbalance << ',' << std::setprecision(2)
      << r.partition_rows_per_s << ',' << std::setprecision(3) << r.effective_GBps << ','
      << r.transfer_GBps << ',' << r.pcie_gen << ',' << std::setprecision(0)
      << r.physical_copy_bytes << '\n';
}

double gbps(double bytes, double ms)
{
  if (ms <= 0) { return 0.0; }
  return (bytes / (ms / 1e3)) / 1e9;
}

std::vector<std::shared_ptr<cucascade::data_batch>> hash_partition_on(
  cucascade::data_batch& input,
  cucascade::memory::memory_space& space,
  rmm::cuda_stream_view stream)
{
  rmm::cuda_set_device_raii guard{rmm::cuda_device_id{space.get_device_id()}};
  auto ro = input.to_read_only();
  return sirius::op::gpu_partition_impl::hash_partition(ro, {0}, 2, stream, space);
}

void sync_both(Fixture& fx)
{
  {
    rmm::cuda_set_device_raii guard{rmm::cuda_device_id{fx.gpu0->get_device_id()}};
    fx.stream0->synchronize();
  }
  {
    rmm::cuda_set_device_raii guard{rmm::cuda_device_id{fx.gpu1->get_device_id()}};
    fx.stream1->synchronize();
  }
}

CsvRow run_transfer(Fixture& fx,
                    std::shared_ptr<cucascade::data_batch> const& b0,
                    std::shared_ptr<cucascade::data_batch> const& b1,
                    std::string const& direction,
                    int warmup,
                    int reps)
{
  auto do_once = [&]() {
    nvtx3::scoped_range nvtx_range{"clone_transfer"};
    auto t0 = clock_type::now();
    if (direction == "transfer_0to1") {
      nvtx3::scoped_range r{"clone_0_to_1"};
      auto clone = clone_to_gpu(b0, fx.gpu1, fx.stream1->view());
      (void)clone;
      fx.stream1->synchronize();
    } else if (direction == "transfer_1to0") {
      nvtx3::scoped_range r{"clone_1_to_0"};
      auto clone = clone_to_gpu(b1, fx.gpu0, fx.stream0->view());
      (void)clone;
      fx.stream0->synchronize();
    } else {
      std::shared_ptr<cucascade::data_batch> c01;
      std::shared_ptr<cucascade::data_batch> c10;
      std::thread t01([&] {
        nvtx3::scoped_range r{"clone_0_to_1"};
        c01 = clone_to_gpu(b0, fx.gpu1, fx.stream1->view());
        fx.stream1->synchronize();
      });
      std::thread t10([&] {
        nvtx3::scoped_range r{"clone_1_to_0"};
        c10 = clone_to_gpu(b1, fx.gpu0, fx.stream0->view());
        fx.stream0->synchronize();
      });
      t01.join();
      t10.join();
    }
    auto t1 = clock_type::now();
    return std::chrono::duration<double, std::milli>(t1 - t0).count();
  };

  for (int i = 0; i < warmup; ++i) {
    (void)do_once();
  }
  std::vector<double> samples;
  samples.reserve(static_cast<std::size_t>(reps));
  for (int i = 0; i < reps; ++i) {
    samples.push_back(do_once());
  }

  CsvRow row;
  row.mode        = direction;
  row.copy_path   = copy_path_label(fx.peer_01, fx.peer_10);
  row.pcie_gen    = fx.pcie_gen;
  row.transfer_ms = median_of(samples);
  row.shuffle_ms  = row.transfer_ms;
  double b01      = static_cast<double>(batch_bytes(*b0));
  double b10      = static_cast<double>(batch_bytes(*b1));
  if (direction == "transfer_0to1") {
    row.remote_bytes_0to1 = b01;
    row.transfer_GBps     = gbps(b01, row.transfer_ms);
  } else if (direction == "transfer_1to0") {
    row.remote_bytes_1to0 = b10;
    row.transfer_GBps     = gbps(b10, row.transfer_ms);
  } else {
    row.remote_bytes_0to1 = b01;
    row.remote_bytes_1to0 = b10;
    row.transfer_GBps     = gbps(b01 + b10, row.transfer_ms);
  }
  row.effective_GBps = row.transfer_GBps;
  double remote      = row.remote_bytes_0to1 + row.remote_bytes_1to0;
  row.physical_copy_bytes =
    (row.copy_path == "host_staging" || row.copy_path == "mixed") ? 2.0 * remote : remote;
  return row;
}

CsvRow run_partition(Fixture& fx,
                     std::shared_ptr<cucascade::data_batch> const& b0,
                     std::shared_ptr<cucascade::data_batch> const& b1,
                     int warmup,
                     int reps)
{
  auto do_once = [&]() {
    nvtx3::scoped_range nvtx_range{"partition"};
    std::vector<std::shared_ptr<cucascade::data_batch>> p0;
    std::vector<std::shared_ptr<cucascade::data_batch>> p1;
    auto t0 = clock_type::now();
    std::thread th0([&] { p0 = hash_partition_on(*b0, *fx.gpu0, fx.stream0->view()); });
    std::thread th1([&] { p1 = hash_partition_on(*b1, *fx.gpu1, fx.stream1->view()); });
    th0.join();
    th1.join();
    sync_both(fx);
    auto t1   = clock_type::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    auto rows0_a = batch_rows(*p0[0]);
    auto rows0_b = batch_rows(*p0[1]);
    auto rows1_a = batch_rows(*p1[0]);
    auto rows1_b = batch_rows(*p1[1]);
    double mean0 = 0.5 * static_cast<double>(rows0_a + rows0_b);
    double mean1 = 0.5 * static_cast<double>(rows1_a + rows1_b);
    double imb0  = mean0 > 0 ? std::max(rows0_a, rows0_b) / mean0 : 0;
    double imb1  = mean1 > 0 ? std::max(rows1_a, rows1_b) / mean1 : 0;
    return std::tuple<double, double, double>(
      ms, 0.5 * (imb0 + imb1), static_cast<double>(batch_rows(*b0) + batch_rows(*b1)));
  };

  for (int i = 0; i < warmup; ++i) {
    (void)do_once();
  }
  std::vector<double> ms_s;
  std::vector<double> imb_s;
  double rows = 0;
  for (int i = 0; i < reps; ++i) {
    auto [ms, imb, r] = do_once();
    ms_s.push_back(ms);
    imb_s.push_back(imb);
    rows = r;
  }

  CsvRow row;
  row.mode                 = "partition";
  row.copy_path            = copy_path_label(fx.peer_01, fx.peer_10);
  row.pcie_gen             = fx.pcie_gen;
  row.partition_ms         = median_of(ms_s);
  row.imbalance            = median_of(imb_s);
  row.partition_rows_per_s = row.partition_ms > 0 ? rows / (row.partition_ms / 1e3) : 0;
  return row;
}

CsvRow run_shuffle(Fixture& fx,
                   std::shared_ptr<cucascade::data_batch> const& b0,
                   std::shared_ptr<cucascade::data_batch> const& b1,
                   int warmup,
                   int reps)
{
  struct Sample {
    double partition_ms = 0;
    double transfer_ms  = 0;
    double shuffle_ms   = 0;
    double remote_01    = 0;
    double remote_10    = 0;
    double imbalance    = 0;
  };

  auto do_once = [&]() {
    std::vector<std::shared_ptr<cucascade::data_batch>> p0;
    std::vector<std::shared_ptr<cucascade::data_batch>> p1;

    auto t0 = clock_type::now();
    {
      nvtx3::scoped_range nvtx_range{"partition"};
      std::thread th0([&] { p0 = hash_partition_on(*b0, *fx.gpu0, fx.stream0->view()); });
      std::thread th1([&] { p1 = hash_partition_on(*b1, *fx.gpu1, fx.stream1->view()); });
      th0.join();
      th1.join();
      sync_both(fx);
    }
    auto t_part = clock_type::now();

    // Destination 0 keeps GPU0's partition 0; destination 1 keeps GPU1's partition 1.
    // Remote: GPU0 partition 1 -> GPU1, GPU1 partition 0 -> GPU0.
    std::shared_ptr<cucascade::data_batch> remote_01;
    std::shared_ptr<cucascade::data_batch> remote_10;
    std::size_t bytes_01 = p0.size() > 1 ? batch_bytes(*p0[1]) : 0;
    std::size_t bytes_10 = p1.size() > 0 ? batch_bytes(*p1[0]) : 0;
    std::size_t rows_d0  = batch_rows(*p0[0]) + batch_rows(*p1[0]);
    std::size_t rows_d1  = batch_rows(*p0[1]) + batch_rows(*p1[1]);

    {
      nvtx3::scoped_range nvtx_range{"shuffle_exchange"};
      std::thread t01([&] {
        if (batch_rows(*p0[1]) == 0) { return; }
        nvtx3::scoped_range r{"clone_0_to_1"};
        remote_01 = clone_to_gpu(p0[1], fx.gpu1, fx.stream1->view());
        fx.stream1->synchronize();
      });
      std::thread t10([&] {
        if (batch_rows(*p1[0]) == 0) { return; }
        nvtx3::scoped_range r{"clone_1_to_0"};
        remote_10 = clone_to_gpu(p1[0], fx.gpu0, fx.stream0->view());
        fx.stream0->synchronize();
      });
      t01.join();
      t10.join();
    }
    auto t1 = clock_type::now();

    Sample s;
    s.partition_ms = std::chrono::duration<double, std::milli>(t_part - t0).count();
    s.transfer_ms  = std::chrono::duration<double, std::milli>(t1 - t_part).count();
    s.shuffle_ms   = std::chrono::duration<double, std::milli>(t1 - t0).count();
    s.remote_01    = static_cast<double>(bytes_01);
    s.remote_10    = static_cast<double>(bytes_10);
    double mean    = 0.5 * static_cast<double>(rows_d0 + rows_d1);
    s.imbalance    = mean > 0 ? std::max(rows_d0, rows_d1) / mean : 0;
    (void)remote_01;
    (void)remote_10;
    return s;
  };

  for (int i = 0; i < warmup; ++i) {
    (void)do_once();
  }
  std::vector<double> part, xfer, shuf, r01, r10, imb;
  double total_rows = static_cast<double>(batch_rows(*b0) + batch_rows(*b1));
  for (int i = 0; i < reps; ++i) {
    auto s = do_once();
    part.push_back(s.partition_ms);
    xfer.push_back(s.transfer_ms);
    shuf.push_back(s.shuffle_ms);
    r01.push_back(s.remote_01);
    r10.push_back(s.remote_10);
    imb.push_back(s.imbalance);
  }

  CsvRow row;
  row.mode                 = "shuffle";
  row.copy_path            = copy_path_label(fx.peer_01, fx.peer_10);
  row.pcie_gen             = fx.pcie_gen;
  row.partition_ms         = median_of(part);
  row.transfer_ms          = median_of(xfer);
  row.shuffle_ms           = median_of(shuf);
  row.remote_bytes_0to1    = median_of(r01);
  row.remote_bytes_1to0    = median_of(r10);
  row.imbalance            = median_of(imb);
  row.partition_rows_per_s = row.partition_ms > 0 ? total_rows / (row.partition_ms / 1e3) : 0;
  double remote            = row.remote_bytes_0to1 + row.remote_bytes_1to0;
  row.effective_GBps       = gbps(remote, row.shuffle_ms);
  row.transfer_GBps        = gbps(remote, row.transfer_ms);
  row.physical_copy_bytes =
    (row.copy_path == "host_staging" || row.copy_path == "mixed") ? 2.0 * remote : remote;
  return row;
}

void warmup_pcie(Fixture& fx)
{
  auto tiny0 = make_uniform_batch(*fx.gpu0, fx.stream0->view(), 4096, 1, 1);
  auto tiny1 = make_uniform_batch(*fx.gpu1, fx.stream1->view(), 4096, 1, 2);
  (void)clone_to_gpu(tiny0, fx.gpu1, fx.stream1->view());
  (void)clone_to_gpu(tiny1, fx.gpu0, fx.stream0->view());
  sync_both(fx);
  fx.pcie_gen = query_pcie_gen(fx.gpu0->get_device_id());
}

}  // namespace

int main(int argc, char** argv)
{
  try {
    auto cli = parse_cli(argc, argv);

    int device_count = 0;
    check_cuda(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount");
    if (device_count < 2) {
      std::cerr << "sirius_shuffle_benchmark requires >=2 GPUs; saw " << device_count
                << ". Exiting 0.\n";
      return 0;
    }

    Fixture fx;
    fx.setup();
    std::cerr << "copy_path=" << copy_path_label(fx.peer_01, fx.peer_10)
              << " probe_peer_dma_works 0->1=" << (fx.peer_01 ? "true" : "false")
              << " 1->0=" << (fx.peer_10 ? "true" : "false") << "\n";
    std::cerr << "cudaDeviceCanAccessPeer is not used as proof of DMA; see probe above.\n";

    warmup_pcie(fx);
    std::cerr << "pcie_gen_after_warmup=" << fx.pcie_gen << "\n";

    auto sizes                = parse_size_list(cli.sizes);
    int n_cols                = cli.payload_cols + 1;
    std::size_t bytes_per_row = static_cast<std::size_t>(n_cols) * sizeof(std::int64_t);

    std::ofstream csv_file;
    std::ostream* file_out = nullptr;
    if (!cli.csv_path.empty()) {
      csv_file.open(cli.csv_path);
      if (!csv_file) { throw std::runtime_error("failed to open --csv path"); }
      file_out = &csv_file;
    }
    write_csv_header(std::cout);
    if (file_out) { write_csv_header(*file_out); }

    auto emit = [&](CsvRow row, std::uint64_t size_bytes) {
      row.size_bytes = size_bytes;
      write_csv_row(std::cout, row);
      if (file_out) {
        write_csv_row(*file_out, row);
        file_out->flush();
      }
    };

    bool do_transfer  = cli.mode == "all" || cli.mode == "transfer";
    bool do_partition = cli.mode == "all" || cli.mode == "partition";
    bool do_shuffle   = cli.mode == "all" || cli.mode == "shuffle";
    if (!do_transfer && !do_partition && !do_shuffle) {
      throw std::invalid_argument("unknown --mode " + cli.mode);
    }

    for (auto size_bytes : sizes) {
      auto num_rows =
        std::max<std::size_t>(1, static_cast<std::size_t>(size_bytes) / bytes_per_row);
      std::cerr << "building inputs size_bytes=" << size_bytes << " rows=" << num_rows
                << " cols=" << n_cols << "\n";
      auto b0 =
        make_uniform_batch(*fx.gpu0, fx.stream0->view(), num_rows, cli.payload_cols, 0xC0FFEE);
      auto b1 =
        make_uniform_batch(*fx.gpu1, fx.stream1->view(), num_rows, cli.payload_cols, 0xBADC0DE);
      std::cerr << "  gpu0_bytes=" << batch_bytes(*b0) << " gpu1_bytes=" << batch_bytes(*b1)
                << "\n";

      if (do_transfer) {
        emit(run_transfer(fx, b0, b1, "transfer_0to1", cli.warmup, cli.reps), size_bytes);
        emit(run_transfer(fx, b0, b1, "transfer_1to0", cli.warmup, cli.reps), size_bytes);
        emit(run_transfer(fx, b0, b1, "transfer_bidir", cli.warmup, cli.reps), size_bytes);
      }
      if (do_partition) { emit(run_partition(fx, b0, b1, cli.warmup, cli.reps), size_bytes); }
      if (do_shuffle) { emit(run_shuffle(fx, b0, b1, cli.warmup, cli.reps), size_bytes); }
    }
    return 0;
  } catch (std::exception const& ex) {
    std::cerr << "sirius_shuffle_benchmark: " << ex.what() << "\n";
    return 1;
  }
}
