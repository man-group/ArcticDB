#include <gtest/gtest.h>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/arrow/nanoarrow/nanoarrow.h>
#include <arcticdb/util/test/generators.hpp>

TEST(Arrow, RoundtripArray) {
    using namespace arcticdb;
    const auto type = make_scalar_type(DataType::UINT64);
    Column column{type, 10, false, false};
    FieldWrapper field_wrapper{type, "numbers"};
    for(auto i = 0ULL; i < 10; ++i) {
        column.set_scalar<uint64_t>(i, i);
    }

    Schema schema;
    //arrow_schema_from_field(schema.ptr(), field_wrapper.field());
    Array array;
    column_to_arrow_array(array, column, field_wrapper.field());

    const auto arrow_type = arcticdb_to_arrow_type(column.type().data_type());
    auto output_column = arrow_array_to_column(array, arrow_type);
    ASSERT_EQ(column, output_column);
}


TEST(Arrow, RoundtripSegment) {
    using namespace arcticdb;
    StreamId symbol("test_arrow");
    auto count = 0LL;
    auto wrapper = SinkWrapper(symbol, {
        scalar_field(DataType::UINT64, "thing1"),
        scalar_field(DataType::UINT64,  "thing2")
    });

    for(auto j = 0; j < 20; ++j ) {
        wrapper.aggregator_.start_row(timestamp(count++))([&](auto &&rb) {
            rb.set_scalar(1, j);
            rb.set_scalar(2, j * 2);
        });
    }

    wrapper.aggregator_.commit();

    auto arr = segment_to_record_batch(wrapper.segment());
    auto seg = record_batch_to_segment(*arr);
    ASSERT_EQ(wrapper.segment(), seg);
}