#include "statement_router.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <array>

namespace sirius_gateway {

using namespace duckdb;

namespace {

/// Set on each rank by the gateway; a client changing one would desynchronize the ranks.
constexpr std::array<char const*, 4> rank_settings{"sirius_distributed_query_id",
                                                   "sirius_exchange_peers",
                                                   "sirius_exchange_rank",
                                                   "enable_duckdb_fallback"};

bool is_gateway_name(std::string const& name)
{
  return StringUtil::StartsWith(name, "sirius_gateway_") || StringUtil::StartsWith(name, "quack_");
}

/// Functions a statement calls at its top level: `CALL f()`, `FROM f()`, `SELECT f()`.
std::vector<std::string> called_functions(SQLStatement& stmt)
{
  std::vector<std::string> names;
  auto add = [&](ParsedExpression& e) {
    if (e.GetExpressionClass() == ExpressionClass::FUNCTION) {
      names.push_back(
        StringUtil::Lower(e.Cast<FunctionExpression>().FunctionName().GetIdentifierName()));
    }
  };
  if (stmt.type == StatementType::CALL_STATEMENT) { add(*stmt.Cast<CallStatement>().function); }
  if (stmt.type == StatementType::SELECT_STATEMENT) {
    auto& node = *stmt.Cast<SelectStatement>().node;
    if (node.type == QueryNodeType::SELECT_NODE) {
      auto& select = node.Cast<SelectNode>();
      if (select.from_table && select.from_table->type == TableReferenceType::TABLE_FUNCTION) {
        add(*select.from_table->Cast<TableFunctionRef>().function);
      }
      for (auto& e : select.select_list) {
        add(*e);
      }
    }
  }
  return names;
}

/// Whether @p stmt runs on the gateway rather than on every rank.
bool runs_on_gateway(SQLStatement& stmt)
{
  if (stmt.type == StatementType::SET_STATEMENT) {
    auto const name = StringUtil::Lower(stmt.Cast<SetStatement>().name.GetIdentifierName());
    for (auto const* owned : rank_settings) {
      if (name == owned) {
        throw InvalidInputException("%s is set on every rank by the gateway", name);
      }
    }
    if (is_gateway_name(name)) {
      throw InvalidInputException("%s can only be set on the gateway's console", name);
    }
  }
  bool local = false;
  for (auto const& name : called_functions(stmt)) {
    if (!is_gateway_name(name)) { continue; }
    if (name != "sirius_gateway_status" && name != "sirius_gateway_log") {
      throw InvalidInputException("%s can only be called on the gateway's console", name);
    }
    local = true;
  }
  return local;
}

}  // namespace

std::string route(std::string const& sql)
{
  Parser parser{ParserOptions::Builtin()};
  try {
    parser.ParseQuery(sql);
  } catch (std::exception&) {
    return sql;  // DuckDB reports the parse error when it runs the text
  }
  std::string routed;
  for (auto& stmt : parser.statements) {
    auto const& at = stmt->stmt_location;
    auto text      = at.offset < sql.size() ? sql.substr(at.offset, at.length) : stmt->query;
    StringUtil::RTrim(text, ";\n\t ");
    routed += runs_on_gateway(*stmt)
                ? text
                : "FROM sirius_gateway_run(" + KeywordHelper::WriteQuoted(text) + ")";
    routed += ";\n";
  }
  return routed.empty() ? sql : routed;
}

}  // namespace sirius_gateway
