/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

inline rc::Gen <arcticdb::entity::DataType> gen_numeric_datatype() {
    return rc::gen::element<arcticdb::entity::DataType>(
        arcticdb::entity::DataType::INT8,
        arcticdb::entity::DataType::UINT8,
        arcticdb::entity::DataType::INT16,
        arcticdb::entity::DataType::UINT16,
        arcticdb::entity::DataType::INT32,
        arcticdb::entity::DataType::UINT32,
        arcticdb::entity::DataType::INT64,
        arcticdb::entity::DataType::UINT64,
        arcticdb::entity::DataType::FLOAT32,
        arcticdb::entity::DataType::FLOAT64,
        arcticdb::entity::DataType::NANOSECONDS_UTC64
    );
}

inline rc::Gen <arcticdb::entity::Dimension> gen_dimension() {
    return rc::gen::element<arcticdb::entity::Dimension>(
        arcticdb::entity::Dimension::Dim0 // Just test scalars for the moment
        //      arcticdb::entity::Dimension::Dim1,
        //      arcticdb::entity::Dimension::Dim2
    );
}

struct TestDataFrame {
    size_t num_columns_;
    size_t num_rows_;
    std::vector<arcticdb::entity::TypeDescriptor> types_;
    std::vector<std::vector<int>> data_;
    arcticdb::entity::timestamp start_ts_;
    std::vector<uint8_t> timestamp_increments_;
    std::vector<std::string> column_names_;
};

namespace rc {
template<>
struct Arbitrary<arcticdb::entity::TypeDescriptor> {
    static Gen <arcticdb::entity::TypeDescriptor> arbitrary() {
        return gen::build<arcticdb::entity::TypeDescriptor>(
            gen::set(&arcticdb::entity::TypeDescriptor::data_type_, gen_numeric_datatype()),
            gen::set(&arcticdb::entity::TypeDescriptor::dimension_, gen_dimension())
        );
    }
};

/*
 * FieldDescriptors are given unique names. */
template<>
struct Arbitrary<arcticdb::entity::StreamDescriptor> {
    static Gen <arcticdb::entity::StreamDescriptor> arbitrary() {
        using namespace arcticdb::entity;
        const auto id = *gen::arbitrary<std::string>();
        const auto num_fields = *gen::arbitrary<size_t>();
        const auto field_names = *gen::container<std::unordered_set<std::string>>(num_fields, gen::nonEmpty(gen::string<std::string>()));
        arcticdb::FieldCollection field_descriptors;
        for (const auto& field_name: field_names) {
            field_descriptors.add_field(arcticdb::entity::scalar_field(*gen_numeric_datatype(), field_name));
        }
        auto desc =stream_descriptor(arcticdb::entity::StreamId{id}, RowCountIndex{}, arcticdb::fields_from_range(field_descriptors));
        return gen::build<StreamDescriptor>(
            gen::set(&StreamDescriptor::data_, gen::just(desc.data_)),
            gen::set(&StreamDescriptor::fields_, gen::just(desc.fields_))
        );
    }
};

//TODO rework this, it sucks
template<>
struct Arbitrary<TestDataFrame> {
    static Gen <TestDataFrame> arbitrary() {
        auto num_rows = *rc::gen::inRange(2, 10);
        auto num_columns = *rc::gen::inRange(2, 10);
        return gen::build<TestDataFrame>(
            gen::set(&TestDataFrame::num_columns_, gen::just(num_columns)),
            gen::set(&TestDataFrame::num_rows_, gen::just(num_rows)),
            gen::set(&TestDataFrame::types_,
                     gen::container<std::vector<arcticdb::entity::TypeDescriptor>>(num_rows,
                                                                                  rc::gen::arbitrary<arcticdb::entity::TypeDescriptor>())),
            gen::set(&TestDataFrame::data_,
                     gen::container<std::vector<std::vector<int>>>(num_columns,
                                                                   gen::container<std::vector<int>>(num_rows,
                                                                                                    gen::inRange(0,
                                                                                                                 100)))),
            gen::set(&TestDataFrame::start_ts_, gen::inRange(1, 100)),
            gen::set(&TestDataFrame::timestamp_increments_,
                     gen::container<std::vector<uint8_t>>(num_rows, gen::inRange(1, 100))),
            gen::set(&TestDataFrame::column_names_,
                     gen::container<std::vector<std::string>>(num_columns, gen::nonEmpty(gen::string<std::string>())))
        );
    }
};

}

namespace as = arcticdb::stream;
namespace ac = arcticdb::entity;

inline as::FixedSchema schema_from_test_frame(const TestDataFrame &data_frame, StreamId stream_id) {
    arcticdb::FieldCollection fields;
    for (size_t i = 0; i < data_frame.num_columns_; ++i)
        fields.add_field(arcticdb::entity::scalar_field(data_frame.types_[i].data_type(), data_frame.column_names_[i]));

    const auto index = as::TimeseriesIndex::default_index();
    return as::FixedSchema{
        index.create_stream_descriptor(stream_id, fields), index
    };
}

inline IndexRange test_frame_range(const TestDataFrame &data_frame) {
    return IndexRange{data_frame.start_ts_, std::accumulate(data_frame.timestamp_increments_.begin(),
                                                            data_frame.timestamp_increments_.end(),
                                                            data_frame.start_ts_)};
}

template<typename WriterType>
folly::Future<arcticdb::entity::VariantKey> write_frame_data(const TestDataFrame &data_frame, WriterType &writer) {
    auto timestamp = data_frame.start_ts_ + 1;
    for (size_t row = 0; row < data_frame.num_rows_; ++row) {
        writer.start_row(timestamp)([&](auto &&rb) {
            for (size_t col = 0; col < data_frame.num_columns_; ++col) {
                data_frame.types_[col].visit_tag([&](auto type_desc_tag) {
                    using raw_type = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                    using data_type_tag  =  typename decltype(type_desc_tag)::DataTypeTag;
                    if (is_sequence_type(data_type_tag::data_type
                    ))
                        rb.set_string(col + 1, fmt::format("{}", data_frame.data_[col][row]));
                    else
                        rb.set_scalar(col + 1, raw_type(data_frame.data_[col][row]));
                });
            }
        });
        timestamp += data_frame.timestamp_increments_[row];
    }

    return writer.commit();
}

inline folly::Future<arcticdb::entity::VariantKey> write_test_frame(StreamId stream_id,
                                                                const TestDataFrame &data_frame,
                                                                std::shared_ptr<StreamSink> store) {
    auto schema = schema_from_test_frame(data_frame, stream_id);
    auto start_end = test_frame_range(data_frame);
    auto gen_id = arcticdb::VersionId(0);

    StreamWriter<TimeseriesIndex, FixedSchema> writer{
        std::move(schema),
        store,
        gen_id,
        start_end,
        RowCountSegmentPolicy{4}
    };

    return write_frame_data(data_frame, writer);
}

template<typename ReaderType>
bool check_read_frame(const TestDataFrame &data_frame, ReaderType &reader, std::vector<std::string> &errors) {
    bool success = true;
    auto timestamp = data_frame.start_ts_;
    auto row = 0u;

    reader.foreach_row([&row, &timestamp, &success, &data_frame, &errors](auto &&row_ref) {
        timestamp += data_frame.timestamp_increments_[row];
        for (size_t col = 0; col < data_frame.num_columns_; ++col) {
            data_frame.types_[col].visit_tag([&](auto type_desc_tag) {
                arcticdb::entity::DataType dt = TypeDescriptor(type_desc_tag).data_type();
                arcticdb::entity::DataType
                    stored_dt = row_ref.segment().column_descriptor(col + 1).type().data_type();
                if (dt != stored_dt) {
                    errors.push_back(fmt::format("Type mismatch {} != {} at pos {}:{}", dt, stored_dt, col, row));
                    success = false;
                }
                auto dimension = static_cast<uint64_t>(TypeDescriptor(type_desc_tag).dimension());
                auto stored_dimension = row_ref.segment().column_descriptor(col + 1).type().dimension();
                if (dimension != static_cast<uint64_t>(stored_dimension)) {
                    errors.push_back(fmt::format("Dimension mismatch {} != {} at pos {}:{}",
                                                 dimension,
                                                 stored_dimension,
                                                 col,
                                                 row));
                    success = false;
                }

                using raw_type = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                auto value = raw_type(data_frame.data_[col][row]);
                auto stored_value = row_ref.template scalar_at<raw_type>(col + 1).value();
                if (value != stored_value) {
                    errors.push_back(fmt::format("Value mismatch {} != {} at pos {}:{}",
                                                 value,
                                                 stored_value,
                                                 col,
                                                 row));
                    success = false;
                }
            });
        }
        ++row;
    });
    return success;
}

inline bool check_test_frame(const TestDataFrame &data_frame,
                             const arcticdb::entity::AtomKey &key,
                             std::shared_ptr<StreamSource> store,
                             std::vector<std::string> &errors) {
    StreamReader<arcticdb::entity::AtomKey, folly::Function<std::vector<arcticdb::entity::AtomKey>()>, arcticdb::SegmentInMemory::Row> stream_reader{
        [&]() { return std::vector<arcticdb::entity::AtomKey>{key}; },
        store
    };

    return check_read_frame(data_frame, stream_reader, errors);
}

namespace arrow {
    class Decimal128;
}

namespace rc {
    template<>
    struct Arbitrary<arrow::Decimal128> {
        static Gen<arrow::Decimal128> arbitrary();
    };
}

rc::Gen<std::string> gen_arrow_decimal128_string();