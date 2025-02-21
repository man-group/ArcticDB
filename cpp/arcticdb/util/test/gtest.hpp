#pragma once

#ifdef _WIN32
#include <corecrt_io.h>

// <folly/portability/Unistd.h> also defines a close and makes namespace folly::portability::unistd visible,
// so we specifiy that we want the close in the global namespace, else <gtest-port.h> cannot resolve the
// implementation. This problem seems to be Windows specific.

/*If you see the error message below, this might be the header file you're looking for:

[build] gtest/internal/gtest-port.h(2075): error C2668: 'close': ambiguous call to overloaded function
[build] C:\Program Files (x86)\Windows Kits\10\include\10.0.22000.0\ucrt\corecrt_io.h(461): note: could be 'int
close(int)' [build] folly/portability/Unistd.h(76): note: or       'int folly::portability::unistd::close(int)' [build]
gtest/internal/gtest-port.h(2075): note: while trying to match the argument list '(int)'
*/
namespace testing::internal::posix {
inline int close(int i) { return ::close(i); }
} // namespace testing::internal::posix
#endif // _WIN32

#include <gtest/gtest.h>
