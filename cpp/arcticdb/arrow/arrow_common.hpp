#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/arrow/nanoarrow/nanoarrow.hpp>


namespace arcticdb {
inline ArrowType arcticdb_to_arrow_type(entity::DataType data_type) {
    using namespace arcticdb::entity;

    switch(data_type) {
    case DataType::UINT8:
        return ArrowType::NANOARROW_TYPE_UINT8;
    case DataType::UINT16:
        return ArrowType::NANOARROW_TYPE_UINT16;
    case DataType::UINT32:
        return ArrowType::NANOARROW_TYPE_UINT32;
    case DataType::UINT64:
        return ArrowType::NANOARROW_TYPE_UINT64;
    case DataType::INT8:
        return ArrowType::NANOARROW_TYPE_INT8;
    case DataType::INT16:
        return ArrowType::NANOARROW_TYPE_INT16;
    case DataType::INT32:
        return ArrowType::NANOARROW_TYPE_INT32;
    case DataType::INT64:
        return ArrowType::NANOARROW_TYPE_INT64;
    case DataType::FLOAT32:
        return ArrowType::NANOARROW_TYPE_UINT8;
    case DataType::FLOAT64:
        return ArrowType::NANOARROW_TYPE_FLOAT;
    case DataType::BOOL8:
        return ArrowType::NANOARROW_TYPE_BOOL;
    case DataType::NANOSECONDS_UTC64:
        return ArrowType::NANOARROW_TYPE_TIME64;
    case DataType::ASCII_FIXED64:
    case DataType::ASCII_DYNAMIC64:
    case DataType::UTF_FIXED64:
    case DataType::UTF_DYNAMIC64:
        return ArrowType::NANOARROW_TYPE_STRING;
//    case DataType::EMPTYVAL:
        //       return ArrowType::NANOARROW_TYPE_UINT8;
    default:
        util::raise_rte("Unknow data type in arrow conversion: {}", data_type);
    }
}

inline ArrowType base_arrow_type(ArrowType arrow_type) {
    switch(arrow_type) {
    case NANOARROW_TYPE_TIME64:
        return NANOARROW_TYPE_INT64;
    default:
        return arrow_type;
    }
}

inline entity::DataType arrow_to_arcticdb_type(ArrowType arrow_type) {
    using namespace arcticdb::entity;
    switch(arrow_type) {
    case ArrowType::NANOARROW_TYPE_UINT8:
        return DataType::UINT8;
    case ArrowType::NANOARROW_TYPE_UINT16:
        return DataType::UINT16;
    case ArrowType::NANOARROW_TYPE_UINT32:
        return DataType::UINT32;
    case ArrowType::NANOARROW_TYPE_UINT64:
        return DataType::UINT64;
    case ArrowType::NANOARROW_TYPE_INT8:
        return DataType::INT8;
    case ArrowType::NANOARROW_TYPE_INT16:
        return DataType::INT16;
    case ArrowType::NANOARROW_TYPE_INT32:
        return DataType::INT32;
    case ArrowType::NANOARROW_TYPE_INT64:
        return DataType::INT64;
    case ArrowType::NANOARROW_TYPE_HALF_FLOAT:
        return DataType::FLOAT32;
    case ArrowType::NANOARROW_TYPE_FLOAT:
        return DataType::FLOAT64;
    case ArrowType::NANOARROW_TYPE_BOOL:
        return DataType::BOOL8;
    case ArrowType::NANOARROW_TYPE_TIME64:
        return DataType::NANOSECONDS_UTC64;
    case ArrowType::NANOARROW_TYPE_STRING:
        return DataType::UTF_DYNAMIC64;
    default:
        util::raise_rte("Unknown arrow type in arrow conversion; {}", arrow_type);
    }
}

#define ARCTICDB_ARROW_CHECK(EXPR) { \
     if(auto ret = EXPR; ret != 0) \
        util::raise_rte("Error {} in arrow expression '{}'", ret, EXPR); \
}
}  //namespace arcticdb