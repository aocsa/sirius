#include <sirius/exception.hpp>

int main() {
  try {
    throw sirius::internal_exception("smoke {}", 1);
  } catch (std::exception const&) {
    return 0;
  }
}
