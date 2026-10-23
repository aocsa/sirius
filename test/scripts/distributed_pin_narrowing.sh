#!/usr/bin/env bash
# Distributed pins with compressed materialization: every rank must plan the same column types.
#
# A pin narrows each column to the smallest carrier its values fit. In a distributed run a rank
# pins only its share of a table's files, so two ranks can hold different value ranges and
# narrow the same column differently. Rank 0 here holds small DECIMAL(15,2) values (a decimal32
# carrier) and rank 1 large ones (native decimal64); table u is one small file that only rank 0
# holds. Queries that move those decimals between ranks must still return DuckDB's answer.
#
# Needs a build with -DSIRIUS_WITH_NIXL=ON and 2 GPUs.
# usage: pixi run bash test/scripts/distributed_pin_narrowing.sh [work_dir]
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="${1:-/tmp/${USER:-sirius}/distributed_pin_narrowing}"
DUCKDB="$REPO/sirius-duckdb/build/release/duckdb"
RANKS=2
# DuckDB changes its output format for AI agents; compare plain CSV.
unset AI_AGENT CLAUDECODE CLAUDE_CODE
rm -rf "$WORK" && mkdir -p "$WORK/data/t" "$WORK/data/u"

# One row group per file. Rank 0's file is the larger, so the byte-range split gives each rank
# exactly one of t's files; u's single row group belongs to rank 0.
"$DUCKDB" -batch <<EOF
COPY (SELECT (i % 1000)::INTEGER AS k, ((i % 10000) / 100.0)::DECIMAL(15,2) AS v
      FROM range(2000000) r(i))
  TO '$WORK/data/t/part0.parquet' (FORMAT parquet, ROW_GROUP_SIZE 4000000);
COPY (SELECT (i % 1000)::INTEGER AS k, (50000000000 + i)::DECIMAL(15,2) AS v
      FROM range(1000000) r(i))
  TO '$WORK/data/t/part1.parquet' (FORMAT parquet, ROW_GROUP_SIZE 4000000);
COPY (SELECT i::INTEGER AS k, (i * 7.25)::DECIMAL(15,2) AS w FROM range(1000) r(i))
  TO '$WORK/data/u/part0.parquet' (FORMAT parquet);
EOF

views="create view t as select * from read_parquet('$WORK/data/t/*.parquet');
create view u as select * from read_parquet('$WORK/data/u/*.parquet');"
queries="select v from t order by v desc, k limit 5;
select count(*), sum(t.v), sum(u.w) from t join u on t.k = u.k;
select k, sum(v) as s from t group by k order by s desc, k limit 5;
select t.k, t.v, u.w from t join u on t.k = u.k order by t.v desc, t.k limit 5;"

# CSV mode ends lines with CRLF: compare without the CRs.
{ echo "$views"; echo ".mode csv"; echo "$queries"; } | "$DUCKDB" -batch | tr -d '\r' \
  > "$WORK/expected.csv"

{
  echo "$views"
  echo ".mode csv"
  echo "SET enable_compressed_materialization = true;"
  echo "CALL pin_table('$WORK/data/t/*.parquet', name = 't', tier = 'host');"
  echo "CALL pin_table('$WORK/data/u/*.parquet', name = 'u', tier = 'host');"
  echo "$queries"
} > "$WORK/ranks.sql"
export SIRIUS_LOG_DIR="$WORK/logs"
rc=0
EXCHANGE_TIMEOUT=300 "$REPO/scripts/exchange_sql_ranks.sh" "$RANKS" "$WORK/ranks.sql" 34000 "$WORK" \
  > /dev/null || rc=$?

# Rank 0's result rows, without the pins' output and log lines.
tr -d '\r' < "$WORK/exchange_rank_0.log" \
  | grep -v -E '^\[|no version information|^Success$|^true$|duckdb agent mode' > "$WORK/actual.csv" || true
if [ "$rc" -ne 0 ] || ! diff -u "$WORK/expected.csv" "$WORK/actual.csv"; then
  echo "FAIL: ranks returned an error or a different answer (rc=$rc); logs in $WORK"
  grep -h -i -E "error|differently" "$WORK"/exchange_rank_*.log | sort | uniq -c | head -5
  exit 1
fi
hits=$(grep -h -c "assigned pinned entry" "$WORK"/logs/*.log | paste -sd+ | bc)
if [ "${hits:-0}" -eq 0 ]; then
  echo "FAIL: no scan was served from a pin; the test did not exercise narrowed pins"
  exit 1
fi
echo "PASS: $RANKS ranks match DuckDB, $hits scans served from pins"
