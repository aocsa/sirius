#pragma once

#include <memory>

namespace sirius::ffi {

class Context {
 public:
  Context();
  ~Context();
};

std::unique_ptr<Context> make_context();

}  // namespace sirius::ffi
