/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/bitset.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/python/python_types.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h> // for auto return type conversion

#include <folly/Function.h>
#include <folly/ScopeGuard.h>

#include <typeinfo>

namespace arcticdb::stream {

template <typename IndexType, typename SchemaType>
inline IndexType get_index_from_schema(const SchemaType& schema) {
    util::check(std::holds_alternative<IndexType>(schema.index()), "Schema and aggregator index type mismatch");
    return std::get<IndexType>(schema.index());
}

template<class Index, class Schema, class Aggregator>
class RowBuilder {
  public:
    using IndexType = Index;
    using SchemaType = Schema;
    using SelfType = RowBuilder<Index, Schema, Aggregator>;

    explicit RowBuilder(Schema& schema, Aggregator& aggregator) :
        schema_(schema),
        index_(get_index_from_schema<IndexType, SchemaType>(schema_)),
        aggregator_(aggregator),
        nbytes_(0) {
    }

    RowBuilder(RowBuilder &) = delete;
    RowBuilder &operator=(RowBuilder &&) = delete;
    RowBuilder &operator=(RowBuilder &) = delete;

    template<class...Args>
    void start_row(const Args...args) {
        reset();
        if constexpr(sizeof...(Args)> 0 && !std::is_same_v<Index, EmptyIndex>) {
            index().set([&](std::size_t pos, auto arg) {
                if constexpr (std::is_integral_v<decltype(arg)> || std::is_floating_point_v<decltype(arg)>)
                    set_scalar_impl(pos, arg);
                else
                    set_string_impl(pos, arg);
            }, args...);
        }
    }

    template<class RowBuilderSetter>
    void operator()(RowBuilderSetter &&setter) {
        SCOPE_FAIL {
            rollback_row();
        };
        setter(*this);
        end_row();
    }

    void rollback_row() noexcept {
        reset();
    }

    void end_row() {
        SCOPE_FAIL {
            rollback_row();
        };

        aggregator_.end_row();
    }

    SelfType &self() {
        return *this;
    }

    [[nodiscard]] std::optional<std::size_t> find_field(std::string_view field_name) const {
        return descriptor().find_field(field_name);
    }

    template<class T,
        std::enable_if_t< std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0
    >
    void set_array(std::size_t pos, py::array_t<T> &val) {
        ARCTICDB_SAMPLE(RowBuilderSetArray, 0)
        magic_.check();
        auto info(val.request());
        auto td = get_type_descriptor(info);
        util::check_arg(pos >= index().field_count(),
                        "expected position > {} (field count), actual {} in set_array", index().field_count(), pos);
        schema_.check(pos, td);
        aggregator_.set_array(pos, val);
        nbytes_ += val.nbytes() + sizeof(shape_t) * val.ndim();
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_scalar(std::size_t pos, T val) {
        util::check_arg(pos >= index().field_count(),
                        "expected position > {} (field count), actual {} in set_scalar", index().field_count(), pos);
        util::check_range(pos, descriptor().field_count(), "No such position in schema");
        set_scalar_impl(pos, val);
    }

    template<class T, std::enable_if_t<std::is_same_v<std::decay_t<T>, std::string>, int> = 0>
    void set_scalar(std::size_t pos, T val) {
        set_string(pos, val);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_scalar_by_name(std::string_view name, T val, DataType data_type) {
        aggregator_.set_scalar_by_name(name, val, data_type);
        nbytes_ += sizeof(T);
    }

    template<class T, std::enable_if_t<std::is_same_v<std::decay_t<T>, std::string_view>, int> = 0>
    void set_scalar_by_name(std::string_view name, T val, DataType data_type) {
                aggregator_.set_string_by_name(name, val, data_type);
    }

    void set_string(std::size_t pos, const std::string &str) {
        check_pos(pos);
        set_string_impl(pos, str);
    }

    void set_string_impl(std::size_t pos, const std::string &str) {
        aggregator_.set_string(pos, str);
    }

    void set_string_array(std::size_t pos, py::array &arr) {
        auto info = arr.request();
        schema_.check(pos, TypeDescriptor(DataType::ASCII_FIXED64, Dimension::Dim1));
        util::check_arg(info.strides.size() == 1, "Assumed numpy string array has no strides");
        util::check_arg(info.shape.size() == 1, "Assumed numpy string array has no shapes");
        util::check_arg(info.itemsize == info.strides[0], "Non-contiguous string arrays not currently supported");
        aggregator_.set_string_array(pos, info.itemsize, info.shape[0], reinterpret_cast<char *>(info.ptr));
    }

    void set_string_list(std::size_t pos, std::vector<std::string> input) {
        schema_.check(pos, TypeDescriptor(DataType::ASCII_DYNAMIC64, Dimension::Dim1));
        aggregator_.set_string_list(pos, input);
    }

    [[nodiscard]] std::size_t nbytes() const {
        return std::size_t(nbytes_);
    }

    Aggregator &aggregator() {
        return *aggregator_;
    }

    [[nodiscard]] const arcticdb::entity::StreamDescriptor &descriptor() const {
        return aggregator_.descriptor();
    }

  private:
    void reset() {
        nbytes_ = 0;
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_block(std::size_t pos, T *val, size_t size) {
        descriptor().fields[pos].type_desc.visit_tag([&](auto &&tag) {
            using DT = std::decay_t<decltype(tag)>;
            using RawType = typename DT::DataTypeTag::raw_type;
            if constexpr (std::is_same_v<typename DT::DimensionTag, DimensionTag<Dimension::Dim0>>) {
                aggregator_.set_block(pos, val, size);
                nbytes_ += sizeof(RawType) * size;
            } else {
                throw std::runtime_error("Can't set non-scalar block");
            }
        });
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_scalar_impl(std::size_t pos, T val) {
        visit_field(descriptor().fields(pos), [&](auto &&tag) {
            using RawType = typename std::decay_t<decltype(tag)>::DataTypeTag::raw_type;

            if constexpr (std::is_same_v<typename std::decay_t<decltype(tag)>::DimensionTag, DimensionTag<Dimension::Dim0>>) {
                RawType conv_val;
                if constexpr ((std::is_integral_v<T> || std::is_floating_point_v<RawType>) &&
                    sizeof(RawType) >= sizeof(T)) {
                    conv_val = val;
                    aggregator_.set_scalar(pos, conv_val);
                    nbytes_ += sizeof(RawType);
                } else {
                    throw ArcticCategorizedException<ErrorCategory::INTERNAL>(fmt::format(
                        "Expected type_descriptor={}, type={}; actual value={}, type {}",
                        descriptor().fields(pos).type(), typeid(conv_val).name(),
                        val, typeid(val).name()));
                }
            } else {
                throw ArcticCategorizedException<ErrorCategory::INTERNAL>(fmt::format(
                    "Expected type_descriptor={}; actual scalar cpp_type={}, value={}",
                    TypeDescriptor{tag}, typeid(val).name(), val));
            }
        });
    }
    
    const IndexType& index() const {
        return index_;
    }

    IndexType& index() {
        return index_;
    }

    void check_pos(std::size_t pos) {
        util::check_arg(pos >= index().field_count(),
                        "expected position > {} (field count), actual {} in set_string (view)", index().field_count(), pos);
        const auto& td = aggregator_.descriptor()[pos];
        util::check_arg(
                is_sequence_type(td.type().data_type()),
                "Set string called on non-string type column");
    }

  private:
    Schema& schema_;
    IndexType index_;
    Aggregator& aggregator_;
    std::uint32_t nbytes_;
    util::MagicNum<'R', 'b', 'l', 'd'> magic_;
};

}
