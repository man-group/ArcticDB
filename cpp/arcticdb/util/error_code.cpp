/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/error_code.hpp>

#ifdef ARCTICDB_USING_CONDA
    #include <semimap/semimap.h>
#else
    #include <third_party/semimap/semimap.h>
#endif

namespace arcticdb {

struct ErrorMapTag{};
using ErrorCodeMap = semi::static_map<int, ErrorCodeData, ErrorMapTag>;

#define ERROR_ID(x) []() constexpr { return static_cast<int>(x); }
ErrorCodeData get_error_code_data(ErrorCode code) {
    #define ERROR_CODE(code, Name) ErrorCodeMap::get(ERROR_ID(code)) = error_code_data<ErrorCode::Name>;
        ARCTIC_ERROR_CODES
    #undef ERROR_CODE

    return ErrorCodeMap::get(static_cast<int>(code));
}

} //namespace arcticdb