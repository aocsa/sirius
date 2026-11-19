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

#include "exchange/stage_choice.hpp"

#include "data/data_batch_utils.hpp"
#include "exchange/all_gather.hpp"
#include "exchange/exchange_service.hpp"
#include "sirius/exception.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/table/table.hpp>
#include <cudf/utilities/error.hpp>

#include <cucascade/data/data_batch.hpp>
#include <cucascade/memory/memory_space.hpp>

#include <limits>
#include <utility>
#include <vector>

namespace sirius::exchange {

stage_count::stage_count(std::shared_ptr<exchange_service> service,
                         stream_id stream,
                         cucascade::memory::memory_space& gpu,
                         std::chrono::milliseconds timeout)
  : _service(std::move(service)), _stream(stream), _gpu(&gpu), _timeout(timeout)
{
  if (!_service) { throw sirius::invalid_input_exception("stage count: no exchange service"); }
}

void stage_count::over()
{
  {
    std::lock_guard lock{_mutex};
    if (_state != state::open) { return; }
    _state = state::resolved;
  }
  _changed.notify_all();
  leave_all_gather(*_service, _stream);
}

std::optional<std::uint64_t> stage_count::total(std::uint64_t mine, ::cuda::stream_ref stream)
{
  {
    std::unique_lock lock{_mutex};
    if (_state != state::open) { return answer(lock); }
    _state = state::gathering;
  }
  std::optional<std::uint64_t> sum;
  std::exception_ptr error;
  try {
    sum = gather(mine, stream);
  } catch (...) {
    error = std::current_exception();
  }
  std::unique_lock lock{_mutex};
  _sum   = sum;
  _error = error;
  _state = state::resolved;
  _changed.notify_all();
  return answer(lock);
}

bool stage_count::resolved() const
{
  std::lock_guard lock{_mutex};
  return _state == state::resolved;
}

std::optional<std::uint64_t> stage_count::wait() const
{
  std::unique_lock lock{_mutex};
  return answer(lock);
}

std::optional<std::uint64_t> stage_count::answer(std::unique_lock<std::mutex>& lock) const
{
  if (!_changed.wait_for(lock, _timeout, [this] { return _state == state::resolved; })) {
    throw sirius::internal_exception(
      "stage count on stream {}: no answer in {} ms", _stream, _timeout.count());
  }
  if (_error) { std::rethrow_exception(_error); }
  return _sum;
}

std::optional<std::uint64_t> stage_count::gather(std::uint64_t mine, ::cuda::stream_ref stream)
{
  auto const mr = _gpu->get_default_allocator();
  cudf::numeric_scalar<std::int64_t> const value{static_cast<std::int64_t>(mine), true, stream, mr};
  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(cudf::make_column_from_scalar(value, 1, stream, mr));
  auto batch =
    make_data_batch(std::make_unique<cudf::table>(std::move(columns)), *_gpu, stream, {});
  auto const counts =
    all_gather(*_service, _stream, std::move(batch), *_gpu, stream.get(), _timeout);

  // A rank that went over left no table.
  std::vector<std::int64_t> theirs(counts.size());
  for (std::size_t r = 0; r < counts.size(); ++r) {
    if (!counts[r]) { return std::nullopt; }
    CUDF_CUDA_TRY(cudaMemcpyAsync(&theirs[r],
                                  get_cudf_table_view(*counts[r]).column(0).data<std::int64_t>(),
                                  sizeof(std::int64_t),
                                  cudaMemcpyDeviceToHost,
                                  stream.get()));
  }
  CUDF_CUDA_TRY(cudaStreamSynchronize(stream.get()));
  std::uint64_t sum = 0;
  for (auto const bytes : theirs) {
    auto const add = static_cast<std::uint64_t>(bytes);
    sum            = add > std::numeric_limits<std::uint64_t>::max() - sum
                       ? std::numeric_limits<std::uint64_t>::max()
                       : sum + add;
  }
  return sum;
}

}  // namespace sirius::exchange
