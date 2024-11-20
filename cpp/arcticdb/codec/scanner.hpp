#pragma once

#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/column_store/column_data.hpp>


namespace arcticdb {
struct IntegerScanner {
    BlockCodecImpl scan(const ColumnData) {

    }
};

struct FloatScanner {
    BlockCodecImpl scan(const ColumnData) {

    }
};

struct StringScanner {
    BlockCodecImpl scan(const ColumnData) {
        return codec::default_lz4_codec();
    }
};

} // namespace arcticdb