#!/usr/bin/env bash
# Profile page for a distributed Q5 run: per-pipeline timings by stage, and per-rank timelines of
# the cold (first) and warm (third) iterations, from each rank's Quent telemetry.
# usage: bash bench/dist-tpch/profile_q5.sh OUT [TELEMETRY_DIR...]
#   OUT            run_tpch.sh's output directory; the page goes to OUT/q5-profile.html
#   TELEMETRY_DIR  one per rank (default: the RANKS newest under OUT/telemetry_data, which are the
#                  last run's; run Q5 last, e.g. QUERIES=5 CASES=pinned run_tpch.sh)
# The page's charts, tables and timings come from the telemetry. Its prose (template.html)
# describes the reference run in README.md: edit it for another run.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${1:?usage: profile_q5.sh OUT [TELEMETRY_DIR...]}"
shift
RANKS="${RANKS:-$(nvidia-smi -L | wc -l)}"
dirs=("$@")
if [ ${#dirs[@]} -eq 0 ]; then
  mapfile -t dirs < <(ls -1dt "$OUT"/telemetry_data/*/ | head -n "$RANKS")
fi
python3 "$HERE/extract.py" --identify "${dirs[@]}"
labels=$(python3 "$HERE/extract.py" --auto "$OUT/q5-prof.json" "${dirs[@]}")
python3 "$HERE/build_page.py" "$OUT/q5-prof.json" "$OUT/q5-profile.html" "$labels"
echo "profile: $OUT/q5-profile.html"
