#include "sirius/ffi.hpp"

namespace sirius::ffi {

Context::Context()  = default;
Context::~Context() = default;

std::unique_ptr<Context> make_context() { return std::make_unique<Context>(); }

}  // namespace sirius::ffi
