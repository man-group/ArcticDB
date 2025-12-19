#pragma once

// LMDB++ is using `std::is_pod` in `lmdb++.h`, which is deprecated as of C++20.
// See: https://github.com/drycpp/lmdbxx/blob/0b43ca87d8cfabba392dfe884eb1edb83874de02/lmdb%2B%2B.h#L1068
// See: https://en.cppreference.com/w/cpp/types/is_pod
// This suppresses the warning.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <lmdb++.h>
#pragma GCC diagnostic pop