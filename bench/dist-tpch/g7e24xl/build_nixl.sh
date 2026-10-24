#!/usr/bin/env bash
# Build NIXL v1.5.0 (UCX plugin only) into ~/tools/nvda_nixl against the pixi env's CUDA 13.4 and
# CUDA-enabled UCX 1.20.1, for sm_120 (RTX PRO 6000 Blackwell).
# usage, from the repo root: pixi run bash bench/dist-tpch/g7e24xl/build_nixl.sh
# Two local changes to the NIXL source: the Python bindings are skipped (Sirius links the C++
# library only; they would pick up the system Python), and -Werror is off (the pixi env's abseil
# deprecates Mutex::Lock). meson, ninja and pybind11 come from a venv in ~/tools/meson-venv.
set -euxo pipefail
export PATH=$HOME/tools/meson-venv/bin:$PATH
export PKG_CONFIG_PATH=$CONDA_PREFIX/lib/pkgconfig
export CUDA_PATH=$CONDA_PREFIX
[ -d $HOME/tools/meson-venv ] || { python3 -m venv $HOME/tools/meson-venv && $HOME/tools/meson-venv/bin/pip -q install meson ninja pybind11 pyyaml; }
[ -d $HOME/tools/src/nixl ] || git clone -b v1.5.0 https://github.com/ai-dynamo/nixl.git $HOME/tools/src/nixl
cd $HOME/tools/src/nixl
sed -i "s/^subdir('bindings')/# subdir('bindings')  # local: Sirius needs only the C++ library/" src/meson.build
rm -rf build
meson setup build --prefix=$HOME/tools/nvda_nixl --buildtype=release -Dwerror=false -Dcpp_args=-Wno-deprecated-declarations \
  -Ducx_path=$CONDA_PREFIX \
  -Dcudapath_inc=$CONDA_PREFIX/targets/x86_64-linux/include \
  -Dcudapath_lib=$CONDA_PREFIX/targets/x86_64-linux/lib \
  -Dcudapath_stub=$CONDA_PREFIX/targets/x86_64-linux/lib/stubs \
  -Dnixl_cuda_arch_list=120 -Denable_plugins=UCX \
  -Dbuild_tests=false -Dbuild_examples=false -Drust=false -Dwith_trace=false \
  -Dc_link_args="-Wl,-rpath,$CONDA_PREFIX/lib" -Dcpp_link_args="-Wl,-rpath,$CONDA_PREFIX/lib"
nice -n 10 ninja -C build -j 24
ninja -C build install
