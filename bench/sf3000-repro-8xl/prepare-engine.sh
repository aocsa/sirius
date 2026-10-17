#!/usr/bin/env bash
# Build one of the two engines used for the 8xl-sf3k reproduction on a GB200. See COMMITS.md.
#
#   bash prepare-engine.sh integration <dir>   # main f32589fb + #2182/#2183/#2204 @0997c1f3 + #2155 @ba9cffa4
#   bash prepare-engine.sh historical  <dir>   # main 38fe9ba2 + #2204 @10727ae9 + #2155 @4d83cd03 (tree 68fee848)
#
# CPUS pins the build (default: the second NUMA node's cores, so a benchmark on cores 0-31 is
# undisturbed). CUDAARCHS defaults to 100-real for GB200; the g7e package uses 120-real.
set -euo pipefail
mode=${1:?integration|historical}
dir=${2:?target directory (must not exist)}
CPUS=${CPUS:-72-143}
JOBS=${JOBS:-64}
CUDAARCHS=${CUDAARCHS:-100-real}
[ -e "$dir" ] && { echo "ERROR: $dir exists" >&2; exit 1; }

case "$mode" in
  integration)
    base=f32589fb47ff3a1900c7b5c3170a01df104fc4b4
    compression=0997c1f3fe4b15ab3a4f979a922dbd011e9d0932
    s3=ba9cffa4ba578e05f66b923ba643d3258e299f61
    expected_tree=1744e2013ca211c4e1b0a1e2005c0e573bbfa87e   # tree of integ/spill-s3-8xl-sf3k @ 2d8f15cb
    ;;
  historical)
    base=38fe9ba22ac23ab556a9d5cd07d78592c3d3c403
    compression=10727ae9874c7d89bb9179484c6be459d2ced97b
    s3=4d83cd03577a0a320427979baf137e488e485f7c
    expected_tree=68fee8488bdba0a765bc5ae11d62cb1780a5d74d
    ;;
  *) echo "usage: $0 integration|historical <dir>" >&2; exit 2 ;;
esac

git clone --filter=blob:none https://github.com/sirius-db/sirius.git "$dir"
cd "$dir"
git fetch --no-tags origin "$base" "$compression" "$s3"
git switch -c "benchmark/spill-$mode" "$base"
id=(-c user.name='Benchmark reproduction' -c user.email='benchmark@localhost')
git "${id[@]}" merge --no-edit --no-ff "$compression"
git "${id[@]}" merge --no-edit --no-ff "$s3"
tree=$(git rev-parse 'HEAD^{tree}')
echo "source tree: $tree"
if [ -n "$expected_tree" ] && [ "$tree" != "$expected_tree" ]; then
  echo "ERROR: expected tree $expected_tree" >&2; exit 1
fi

git submodule update --init --recursive --jobs 8 -- \
  duckdb cucascade substrait sirius-duckdb/duckdb sirius-duckdb/extension-ci-tools
export PATH="$HOME/.pixi/bin:$PATH"
taskset -c "$CPUS" pixi install --locked
taskset -c "$CPUS" pixi run env CUDAARCHS="$CUDAARCHS" CMAKE_BUILD_PARALLEL_LEVEL="$JOBS" make -j"$JOBS"
pixi run sirius-duckdb/build/release/duckdb --version
