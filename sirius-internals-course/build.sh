#!/usr/bin/env bash
# Assemble and validate the course. Run from any directory.
set -euo pipefail

course_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check_only=false
network=false

for arg in "$@"; do
  case "$arg" in
    --check) check_only=true ;;
    --network) network=true ;;
    *) echo "usage: $0 [--check] [--network]" >&2; exit 2 ;;
  esac
done

tmp_file="$(mktemp "${TMPDIR:-/tmp}/sirius-course-index.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT

LC_ALL=C cat \
  "$course_dir/_base.html" \
  "$course_dir"/modules/[0-9][0-9]-*.html \
  "$course_dir/_footer.html" > "$tmp_file"

validator_args=(--assembled "$tmp_file" --course-dir "$course_dir")
if $network; then validator_args+=(--network); fi
python3 "$course_dir/validate-course" "${validator_args[@]}"

if $check_only; then
  if ! cmp -s "$tmp_file" "$course_dir/index.html"; then
    echo "index.html is stale; run sirius-internals-course/build.sh" >&2
    exit 1
  fi
  echo "Course build is valid and index.html is current."
else
  mv "$tmp_file" "$course_dir/index.html"
  trap - EXIT
  echo "Built and validated $course_dir/index.html"
fi
