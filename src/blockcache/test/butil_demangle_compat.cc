#include <cxxabi.h>

#include <cstdlib>
#include <string>

namespace butil {

std::string demangle(const char* name) {
    int status = 0;
    char* demangled = abi::__cxa_demangle(name, nullptr, nullptr, &status);
    if (status != 0 || demangled == nullptr) {
        return name == nullptr ? std::string() : std::string(name);
    }
    std::string result(demangled);
    std::free(demangled);
    return result;
}

}  // namespace butil
