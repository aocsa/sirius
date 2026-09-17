#include <sirius/ffi.hpp>

extern "C" int sirius_prefix_probe() {
  auto ctx = sirius::ffi::make_context();
  return ctx ? 0 : 1;
}
