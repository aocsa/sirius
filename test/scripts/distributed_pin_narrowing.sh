#!/usr/bin/env bash
# Distributed pins: every rank plans the same column types, a replicated pin needs no exchange,
# and a pin that fails on one rank fails on every rank.
#
# A pin narrows each column to the smallest carrier its values fit. In a distributed run a rank
# pins only its share of a table's files, so two ranks can hold different value ranges and
# narrow the same column differently; the ranks agree on one carrier per column when they pin.
# Here rank 0 holds t's small values and rank 1 its large ones: v narrows only on rank 0 (so it
# is planned native), x narrows on both but to different widths (so both plan rank 1's). Table u
# is one small file, pinned whole on every rank (replicate = true), so joins build from it where
# it is. Table w is one row group: rank 0 holds all of it and rank 1 none, so a memory limit only
# rank 0 can exceed must still fail the pin on rank 1.
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
rm -rf "$WORK" && mkdir -p "$WORK/data/t" "$WORK/data/u" "$WORK/data/w"
fail() { echo "FAIL: $*; logs in $WORK"; exit 1; }

# One row group per file. Rank 0's file is the larger, so the byte-range split gives each rank
# exactly one of t's files; u's and w's single row groups belong to rank 0.
"$DUCKDB" -batch <<EOF
COPY (SELECT (i % 1000)::INTEGER AS k, ((i % 10000) / 100.0)::DECIMAL(15,2) AS v,
             (i % 100)::BIGINT AS x
      FROM range(2000000) r(i))
  TO '$WORK/data/t/part0.parquet' (FORMAT parquet, ROW_GROUP_SIZE 4000000);
COPY (SELECT (i % 1000)::INTEGER AS k, (50000000000 + i)::DECIMAL(15,2) AS v,
             (i % 100000)::BIGINT AS x
      FROM range(1000000) r(i))
  TO '$WORK/data/t/part1.parquet' (FORMAT parquet, ROW_GROUP_SIZE 4000000);
COPY (SELECT i::INTEGER AS k, (i * 7.25)::DECIMAL(15,2) AS w FROM range(1000) r(i))
  TO '$WORK/data/u/part0.parquet' (FORMAT parquet);
COPY (SELECT i::BIGINT AS y FROM range(100000) r(i))
  TO '$WORK/data/w/part0.parquet' (FORMAT parquet);
EOF

views="create view t as select * from read_parquet('$WORK/data/t/*.parquet');
create view u as select * from read_parquet('$WORK/data/u/*.parquet');
create view w as select * from read_parquet('$WORK/data/w/*.parquet');"
queries="select v from t order by v desc, k limit 5;
select count(*), sum(t.v), sum(u.w) from t join u on t.k = u.k;
select k, sum(v) as s from t group by k order by s desc, k limit 5;
select t.k, t.v, u.w from t join u on t.k = u.k order by t.v desc, t.k limit 5;
select sum(x), min(x), max(x) from t;
select u.k, sum(t.x) as s from t join u on t.k = u.k group by u.k order by s desc, u.k limit 3;"

# CSV mode ends lines with CRLF: compare without the CRs.
{ echo "$views"; echo ".mode csv"; echo "$queries"; } | "$DUCKDB" -batch | tr -d '\r' \
  > "$WORK/expected.csv"

run_ranks() {  # SQL_FILE OUT_DIR: rank i's output in OUT_DIR/exchange_rank_i.log
  mkdir -p "$2"
  SIRIUS_LOG_DIR="$2/logs" EXCHANGE_TIMEOUT=300 \
    "$REPO/scripts/exchange_sql_ranks.sh" "$RANKS" "$1" 34000 "$2" > /dev/null
}

# 1. Narrowed pins, one of them replicated, serve every query with DuckDB's answer.
{
  echo "$views"
  echo ".mode csv"
  echo "SET enable_compressed_materialization = true;"
  echo "CALL pin_table('$WORK/data/t/*.parquet', name = 't', tier = 'host');"
  echo "CALL pin_table('$WORK/data/u/*.parquet', name = 'u', tier = 'host', replicate = true);"
  echo "$queries"
} > "$WORK/ranks.sql"
rc=0
run_ranks "$WORK/ranks.sql" "$WORK/pinned" || rc=$?
# Rank 0's result rows, without the pins' output and log lines.
tr -d '\r' < "$WORK/pinned/exchange_rank_0.log" \
  | grep -v -E '^\[|no version information|^Success$|^true$|duckdb agent mode' > "$WORK/actual.csv" || true
if [ "$rc" -ne 0 ] || ! diff -u "$WORK/expected.csv" "$WORK/actual.csv"; then
  grep -h -i -E "error|differently" "$WORK"/pinned/exchange_rank_*.log | sort | uniq -c | head -5
  fail "ranks returned an error or a different answer (rc=$rc)"
fi
hits=$(cat "$WORK"/pinned/logs/*.log | grep -a -c "assigned pinned entry" || true)
[ "${hits:-0}" -gt 0 ] || fail "no scan was served from a pin"
replicated=$(cat "$WORK"/pinned/logs/*.log | grep -a -c -E "[1-9][0-9]* scans of replicated pins" || true)
[ "${replicated:-0}" -gt 0 ] || fail "no query planned a scan of the replicated pin of u"

# 2. A pin only rank 0 cannot fit fails on both ranks, and neither keeps it.
{
  echo "$views"
  echo "SET pin_table_memory_limit ="
  echo "  CASE WHEN current_setting('sirius_exchange_rank') = 0 THEN 0.000001 ELSE 0.9 END;"
  echo "CALL pin_table('$WORK/data/w/*.parquet', name = 'w', tier = 'host');"
} > "$WORK/fail.sql"
run_ranks "$WORK/fail.sql" "$WORK/failed" || true
for ((i = 0; i < RANKS; i++)); do
  grep -q "rank(s) 0 could not pin their share" "$WORK/failed/exchange_rank_$i.log" \
    || fail "rank $i did not fail the pin that only rank 0 could not fit"
done

echo "PASS: $RANKS ranks match DuckDB, $hits scans served from pins, $replicated queries read" \
  "the replicated pin, and a pin one rank could not fit failed on every rank"
