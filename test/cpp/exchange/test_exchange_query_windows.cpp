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

#include "sirius/exception.hpp"
#include "sirius_context.hpp"

#include <catch.hpp>
#include <duckdb.hpp>
#include <utils/sirius_test_env.hpp>

#include <memory>
#include <string>

using window = duckdb::SiriusContext::StandaloneQueryScope;

namespace {

void begin_statement(duckdb::Connection& connection)
{
  duckdb::get_sirius_connection_state(*connection.context)->QueryBegin(*connection.context);
}

}  // namespace

TEST_CASE("distributed queries are numbered on one connection and tied to their window",
          "[exchange][query_windows]")
{
  REQUIRE(sirius::test::g_integration_env != nullptr);
  if (!sirius::test::g_integration_env->is_active()) { sirius::test::g_integration_env->resume(); }
  auto owner =
    std::make_unique<duckdb::Connection>(sirius::test::g_integration_env->make_connection());
  auto other =
    std::make_unique<duckdb::Connection>(sirius::test::g_integration_env->make_connection());
  auto ctx = owner->context->registered_state->Get<duckdb::SiriusContext>("sirius_state");
  REQUIRE(ctx);

  // Planned before its window opens, as transparent execution does: the window adopts it.
  begin_statement(*owner);
  auto const first = ctx->next_distributed_query(*owner->context);
  sirius::query_id_t first_window{};
  {
    window w(*ctx, *owner->context, "distributed_first");
    first_window = w.query_id();
    CHECK(ctx->distributed_query_of(first_window) == first);
  }
  CHECK(ctx->distributed_query_of(first_window) == 0);

  // Planned while its window is open: tied to that window at once.
  begin_statement(*owner);
  {
    window w(*ctx, *owner->context, "distributed_second");
    auto const second = ctx->next_distributed_query(*owner->context);
    CHECK(second == first + 1);
    CHECK(ctx->distributed_query_of(w.query_id()) == second);
  }

  // Another connection's statements would number differently on each rank.
  CHECK_THROWS_AS(ctx->next_distributed_query(*other->context), sirius::invalid_input_exception);

  // Closing the connection hands the role on; numbering continues where it stopped.
  owner.reset();
  auto const third = ctx->next_distributed_query(*other->context);
  CHECK(third == first + 2);
  auto later =
    std::make_unique<duckdb::Connection>(sirius::test::g_integration_env->make_connection());
  CHECK_THROWS_AS(ctx->next_distributed_query(*later->context), sirius::invalid_input_exception);
}

TEST_CASE("a statement planned twice keeps its distributed query number",
          "[exchange][query_windows]")
{
  REQUIRE(sirius::test::g_integration_env != nullptr);
  if (!sirius::test::g_integration_env->is_active()) { sirius::test::g_integration_env->resume(); }
  auto connection =
    std::make_unique<duckdb::Connection>(sirius::test::g_integration_env->make_connection());
  auto ctx = connection->context->registered_state->Get<duckdb::SiriusContext>("sirius_state");
  REQUIRE(ctx);

  begin_statement(*connection);
  auto const first = ctx->next_distributed_query(*connection->context);
  CHECK(ctx->next_distributed_query(*connection->context) == first);
  {
    window w(*ctx, *connection->context, "distributed_rebuilt");
    // Rebuilt at execute, as transparent execution does when this rank's pin registry moved.
    CHECK(ctx->next_distributed_query(*connection->context) == first);
    CHECK(ctx->distributed_query_of(w.query_id()) == first);
  }

  begin_statement(*connection);
  CHECK(ctx->next_distributed_query(*connection->context) == first + 1);
}

TEST_CASE("exchange peers are distinct host:port addresses", "[exchange][query_windows]")
{
  REQUIRE(sirius::test::g_integration_env != nullptr);
  if (!sirius::test::g_integration_env->is_active()) { sirius::test::g_integration_env->resume(); }
  auto connection = sirius::test::g_integration_env->make_connection();
  auto set_peers  = [&](std::string const& peers) {
    return connection.Query("SET sirius_exchange_peers = '" + peers + "'");
  };

  CHECK_FALSE(set_peers(" 127.0.0.1:1 ,127.0.0.1:2,")->HasError());
  CHECK_FALSE(set_peers("")->HasError());
  for (auto const* bad : {"127.0.0.1", ":1", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:1x"}) {
    auto const result = set_peers(std::string{"127.0.0.1:1,"} + bad);
    INFO(bad);
    REQUIRE(result->HasError());
    CHECK_THAT(result->GetError(), Catch::Matchers::ContainsSubstring("is not host:port"));
  }
  auto const twice = set_peers("127.0.0.1:1, 127.0.0.1:1");
  REQUIRE(twice->HasError());
  CHECK_THAT(twice->GetError(), Catch::Matchers::ContainsSubstring("is listed twice"));
}
