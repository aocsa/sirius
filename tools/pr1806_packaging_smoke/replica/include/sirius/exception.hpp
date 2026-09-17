#pragma once

#include <format>
#include <stdexcept>
#include <string>

namespace sirius {

class internal_exception : public std::runtime_error {
 public:
  explicit internal_exception(const std::string& msg) : std::runtime_error(msg) {}

  template <typename... Args>
  explicit internal_exception(std::format_string<Args...> fmt, Args&&... args)
    : std::runtime_error(std::format(fmt, std::forward<Args>(args)...))
  {
  }
};

}  // namespace sirius
