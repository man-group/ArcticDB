/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <cstdint>
#include <limits>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/test/test_utils.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/rapidcheck.hpp>

struct ColumnModel {
    std::vector<uint64_t> data;
};

struct ColumnAppend : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t value_;

    void apply(ColumnModel &s0) const override {
        s0.data.push_back(value_);
    }

    void run(const ColumnModel &, arcticdb::Column &sut) const override {
        auto next_row_id = sut.last_row() + 1;
        sut.set_scalar<uint64_t>(next_row_id, value_);
        RC_ASSERT(*sut.ptr_cast<uint64_t>(next_row_id, sizeof(uint64_t)) == value_);
    }

    void show(std::ostream &os) const override {
        os << "Append(" << value_ << ")";
    }
};

struct ColumnLowerBound : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t value_;

    void run(const ColumnModel& m, arcticdb::Column &sut) const override {
        using namespace arcticdb;
        using TagType = TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension::Dim0>>;
        const auto model_it = std::lower_bound(std::begin(m.data), std::end(m.data), value_);
        const auto sut_it = std::lower_bound(sut.template begin<TagType>(), sut.template end<TagType>(), value_);
        RC_ASSERT(std::distance(std::begin(m.data), model_it) == std::distance(sut.template begin<TagType>(), sut_it));
    }

    void show(std::ostream &os) const override {
        os << "Append(" << value_ << ")";
    }
};

struct ColumnUpperBound : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t value_;

    void run(const ColumnModel& m, arcticdb::Column &sut) const override {
        using namespace arcticdb;
        using TagType = TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension::Dim0>>;
        const auto model_it = std::upper_bound(std::begin(m.data), std::end(m.data), value_);
        const auto sut_it = std::upper_bound(sut.template begin<TagType>(), sut.template end<TagType>(), value_);
        RC_ASSERT(std::distance(std::begin(m.data), model_it) == std::distance(sut.template begin<TagType>(), sut_it));
    }

    void show(std::ostream &os) const override {
        os << "Append(" << value_ << ")";
    }
};
struct ColumnRead : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t position_;

    void checkPreconditions(const ColumnModel &s0) const override {
        RC_PRE(position_ < s0.data.size());
    }

    void run(const ColumnModel &s0, arcticdb::Column &sut) const override {
        RC_ASSERT(*sut.ptr_cast<uint64_t>(position_, sizeof(uint64_t)) == s0.data[position_]);
    }

    void show(std::ostream &os) const override {
        os << "Get(" << position_ << ")";
    }
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

RC_GTEST_PROP(Column, Rapidcheck, ()) {
    ColumnModel initial_state;
    arcticdb::Column sut(TypeDescriptor(DataType::UINT64, Dimension::Dim0), 0, false, false);
    rc::state::check(initial_state,
                     sut,
                     &rc::state::gen::execOneOf<ColumnAppend, ColumnRead, ColumnLowerBound, ColumnUpperBound>);
}

#pragma GCC diagnostic pop