/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/column_store/column_map.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>

#include <boost/iterator/iterator_facade.hpp>
#include <folly/container/Enumerate.h>

namespace google::protobuf
{
    class Any;
}

namespace arcticdb {

class SegmentInMemoryImpl;

namespace {
inline std::shared_ptr<SegmentInMemoryImpl> allocate_sparse_segment(const StreamId& id, const IndexDescriptorImpl& index);

inline std::shared_ptr<SegmentInMemoryImpl> allocate_dense_segment(const StreamDescriptor& descriptor, size_t row_count);

inline void check_output_bitset(const arcticdb::util::BitSet& output,
                                const arcticdb::util::BitSet& filter,
                                const arcticdb::util::BitSet& column_bitset
                                ){
    // TODO: Do this in O(1)
    // The logic here is that the filter bitset defines how the output bitset should look like
    // The set bits in filter decides the row ids in the output. The corresponding values in sparse_map
    // should match output bitset
    auto filter_iter = filter.first();
    arcticdb::util::BitSetSizeType output_pos = 0;
    while(filter_iter != filter.end()) {
        arcticdb::util::check_rte(column_bitset.test(*(filter_iter++)) == output.test(output_pos++),
                                 "Mismatch in output bitset in filter_segment");
    }
}
} // namespace


class SegmentInMemoryImpl {
public:
    struct Location {
        Location(SegmentInMemoryImpl *parent,
                 ssize_t row_id_,
                 size_t column_id) :
                 parent_(parent),
                 row_id_(row_id_),
                 column_id_(column_id) {
        }

        template<class Callable>
        auto visit(Callable &&c) const {
            return entity::visit_field(parent_->descriptor().field(column_id_), [this, c=std::forward<Callable>(c)](auto type_desc_tag) {
                using RawType = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag::raw_type;
                return c(parent_->scalar_at<RawType>(row_id_, column_id_));
            });
        }

        template<class Callable>
        auto visit_string(Callable &&c) const {
            return entity::visit_field(parent_->descriptor().field(column_id_), [this, c = std::forward<Callable>(c)](auto type_desc_tag) {
                using DTT = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag;
                if constexpr(is_sequence_type(DTT::data_type))
                    return c(parent_->string_at(row_id_, position_t(column_id_)));
            });
        }

        template<class Callable>
        auto visit_field(Callable &&c) const {
            const auto& field = parent_->descriptor().field(column_id_);
            return entity::visit_field(field, [&field, this, c = std::forward<Callable>(c)](auto type_desc_tag) {
                using DataTypeTag = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag;
                using RawType = typename DataTypeTag::raw_type;
                if constexpr (is_sequence_type(DataTypeTag::data_type))
                    return c(parent_->string_at(row_id_, position_t(column_id_)), std::string_view{field.name()}, type_desc_tag);
                else if constexpr (is_numeric_type(DataTypeTag::data_type) || is_bool_type(DataTypeTag::data_type))
                    return c(parent_->scalar_at<RawType>(row_id_, column_id_), std::string_view{field.name()}, type_desc_tag);
                else if constexpr(is_empty_type(DataTypeTag::data_type))
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("visit_field does not support empty-type columns");
                else
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("visit_field called with unexpected column type");
            });
        }

        template<class Callable>
        auto visit(Callable &&c) {
            return entity::visit_field(parent_->descriptor().field(column_id_), [this, c=std::forward<Callable>(c)](auto type_desc_tag) {
                using RawType = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag::raw_type;
                return c(parent_->reference_at<RawType>(row_id_, column_id_));
            });
        }

        [[nodiscard]] bool has_value() const {
            return parent_->has_value_at(row_id_, position_t(column_id_));
        }

        template<typename RawType>
        RawType &value() {
            return parent_->reference_at<RawType>(row_id_, column_id_);
        }

        template<typename RawType>
        [[nodiscard]] const RawType &value() const {
            return parent_->reference_at<RawType>(row_id_, column_id_);
        }

        bool operator==(const Location &other) const {
            return row_id_ == other.row_id_ && column_id_ == other.column_id_;
        }

        SegmentInMemoryImpl *const parent_;
        ssize_t row_id_;
        size_t column_id_;
    };

    /*
     * Arctic is fundamentally a column oriented database. The below RowIterator however iterates one row at a time,
     * which is not an optimized operation in a columnar database.
     *
     * As a result this is probably not what you want unless you *know* that it is what you want.
     */
    template<class ValueType>
        class RowIterator
            : public boost::iterator_facade<RowIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
        public:
            RowIterator(SegmentInMemoryImpl *parent,
                        ssize_t row_id_,
                        size_t column_id) :
                        location_(parent, row_id_, column_id) {
            }

            RowIterator(SegmentInMemoryImpl *parent,
                        ssize_t row_id_) :
                        location_(parent, row_id_, 0) {
            }

            template<class OtherValue>
                explicit RowIterator(const RowIterator<OtherValue> &other)
                : location_(other.location_) {}


        private:
            friend class boost::iterator_core_access;

            template<class> friend
                class SegmentIterator;

            template<class OtherValue>
            bool equal(const RowIterator<OtherValue> &other) const {
                return location_ == other.location_;
            }

            void increment() { ++location_.column_id_; }

            void decrement() { --location_.column_id_; }

            void advance(ptrdiff_t n) { location_.column_id_ += n; }

            ValueType &dereference() const {
                return location_;
            }

            mutable Location location_;
        };

    struct Row {
        Row() = default;

        ~Row() = default;
        Row(const Row& other) = default;

        Row(SegmentInMemoryImpl *parent, ssize_t row_id_) :
        parent_(parent),
        row_id_(row_id_) {}

        [[nodiscard]] SegmentInMemoryImpl& segment() const {
            return *parent_;
        }

        [[nodiscard]] const StreamDescriptor &descriptor() const {
            return parent_->descriptor();
        }

        bool operator<(const Row &other) const {
            return entity::visit_field(parent_->field(0), [this, &other](auto type_desc_tag) {
                using RawType =  typename decltype(type_desc_tag)::DataTypeTag::raw_type;
                return parent_->scalar_at<RawType>(row_id_, 0) < other.parent_->scalar_at<RawType>(other.row_id_, 0);
            });
        }

        [[nodiscard]] size_t row_pos() const { return row_id_; }

        template<class IndexType>
        [[nodiscard]] auto index() const {
            using RawType =  typename IndexType::TypeDescTag::DataTypeTag::raw_type;
            return parent_->scalar_at<RawType>(row_id_, 0).value();
        }

        friend void swap(Row &left, Row &right) noexcept {
            using std::swap;

            auto a = left.begin();
            auto b = right.begin();
            for (; a != left.end(); ++a, ++b) {
                util::check(a->has_value() && b->has_value(), "Can't swap sparse column values, unsparsify first?");
                a->visit([&b](auto &val) {
                    using ValType = std::decay_t<decltype(val)>;
                    swap(val, b->value<ValType>());
                });
            }
        }

        Row &operator=(const Row &other) = default;

        Location operator[](int pos) const {
            return Location{parent_, row_id_, size_t(pos)};
        }

        using iterator = RowIterator<Location>;
        using const_iterator = RowIterator<const Location>;

        iterator begin() {
            return {parent_, row_id_};
        }

        iterator end() {
            return {parent_, row_id_, size_t(parent_->descriptor().fields().size())};
        }

        [[nodiscard]] const_iterator begin() const {
            return {parent_, row_id_};
        }

        [[nodiscard]] const_iterator end() const {
            return {parent_, row_id_, size_t(parent_->descriptor().fields().size())};
        }

        bool operator==(const Row &other) const {
            return row_id_ == other.row_id_ && parent_ == other.parent_;
        }

        void swap_parent(const Row &other) {
            parent_ = other.parent_;
        }

        template<class S>
        std::optional<S> scalar_at(std::size_t col) const {
            parent_->check_magic();
            const auto& type_desc = parent_->column_descriptor(col).type();
            std::optional<S> val;
            type_desc.visit_tag([this, col, &val](auto impl) {
                using T = std::decay_t<decltype(impl)>;
                using RawType = typename T::DataTypeTag::raw_type;
                if constexpr (T::DimensionTag::value == Dimension::Dim0) {
                    if constexpr (is_sequence_type(T::DataTypeTag::data_type)) {
                        // test only for now
                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("string type not implemented");
                    } else if constexpr(is_numeric_type(T::DataTypeTag::data_type) || is_bool_type(T::DataTypeTag::data_type)) {
                        if constexpr(std::is_same_v<RawType, S>) {
                            val = parent_->scalar_at<RawType>(row_id_, col);
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Type mismatch in scalar access");
                        }
                    } else if constexpr(is_empty_type(T::DataTypeTag::data_type)) {
                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("scalar_at not supported with empty-type columns");
                    } else {
                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected data type in scalar access");
                    }
                } else {
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Scalar method called on multidimensional column");
                }
            });
            return val;
        }

        [[nodiscard]] std::optional<std::string_view> string_at(std::size_t col) const {
            return parent_->string_at(row_id_, position_t(col));
        }

        SegmentInMemoryImpl *parent_;
        ssize_t row_id_;
    };

    template<class ValueType>
        class SegmentIterator
            : public boost::iterator_facade<SegmentIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
        public:
            using value_type = ValueType;

            explicit SegmentIterator(SegmentInMemoryImpl *parent) :
            row_(parent, 0) {
            }

            ~SegmentIterator() = default;

            SegmentIterator(const SegmentIterator& other) = default;

            SegmentIterator &operator=(const SegmentIterator &other) {
                row_.swap_parent(other.row_);
                row_.row_id_ = other.row_.row_id_;
                return *this;
            }

            SegmentIterator(SegmentInMemoryImpl *parent, ssize_t row_id_) :
            row_(parent, row_id_) {
            }

            template<class OtherValue>
                explicit SegmentIterator(const SegmentIterator<OtherValue> &other)
                : row_(other.row_) {}

        private:
            friend class boost::iterator_core_access;

            template<class> friend
                class SegmentIterator;

            template<class OtherValue>
            bool equal(const SegmentIterator<OtherValue> &other) const {
                return row_ == other.row_;
            }

            std::ptrdiff_t distance_to(const SegmentIterator &other) const {
                return other.row_.row_id_ - row_.row_id_;
            }

            void increment() { ++row_.row_id_; }

            void decrement() { --row_.row_id_; }

            void advance(ptrdiff_t n) { row_.row_id_ += n; }

            ValueType &dereference() const {
                return row_;
            }

            mutable Row row_;
        };

    using iterator = SegmentIterator<Row>;
    using const_iterator = SegmentIterator<const Row>;



    SegmentInMemoryImpl();

    SegmentInMemoryImpl(
        const StreamDescriptor& desc,
        size_t expected_column_size,
        AllocationType presize,
        Sparsity allow_sparse) :
            SegmentInMemoryImpl(
                desc,
                expected_column_size,
                presize,
                allow_sparse,
                OutputFormat::NATIVE,
                DataTypeMode::INTERNAL) {
    }

    SegmentInMemoryImpl(
        const StreamDescriptor& desc,
        size_t expected_column_size,
        AllocationType presize,
        Sparsity allow_sparse,
        OutputFormat output_format,
        DataTypeMode mode);

    ~SegmentInMemoryImpl();

    iterator begin() { return iterator{this}; }

    iterator end() {
        util::check(row_id_ != -1, "End iterator called with negative row id, iterator will never terminate");
        return iterator{this, row_id_ + 1};
    }

    const_iterator begin() const {
        return const_iterator{const_cast<SegmentInMemoryImpl*>(this)};
    }

    const_iterator end() const {
        util::check(row_id_ != -1, "End iterator called with negative row id, iterator will never terminate");
        return const_iterator{const_cast<SegmentInMemoryImpl*>(this), row_id_} ;
    }

    ARCTICDB_MOVE_ONLY_DEFAULT_EXCEPT(SegmentInMemoryImpl)

    void generate_column_map() const;

    void create_columns(
        size_t old_size,
        size_t expected_column_size,
        AllocationType presize,
        Sparsity allow_sparse,
        OutputFormat output_format,
        DataTypeMode mode);

    size_t on_descriptor_change(
        const StreamDescriptor &descriptor,
        size_t expected_column_size,
        AllocationType presize,
        Sparsity allow_sparse,
        OutputFormat output_format,
        DataTypeMode mode
        );

    std::optional<std::size_t> column_index(std::string_view name) const;

    [[nodiscard]] std::optional<std::size_t> column_index_with_name_demangling(std::string_view name) const;

    const Field& column_descriptor(size_t col) {
        return (*descriptor_)[col];
    }

    void end_row() {
        row_id_++;
    }

    const TimeseriesDescriptor& index_descriptor() const {
        util::check(tsd_.has_value(), "Index descriptor requested but not set");
        return *tsd_;
    }

    TimeseriesDescriptor& mutable_index_descriptor() {
        util::check(tsd_.has_value(), "Index descriptor requested but not set");
        return *tsd_;
    }

    void end_block_write(ssize_t size) {
        row_id_ += size;
    }

    void set_offset(ssize_t offset) {
        offset_ = offset;
    }

    ssize_t offset() const {
        return offset_;
    }

    void push_back(const Row &row) {
        for (auto it : folly::enumerate(row)) {
            it->visit([&it, that=this](const auto &val) {
                if(val)
                    that->set_scalar(it.index, val.value());
            });
        }
        end_row();
    }

    void set_value(position_t idx, const Location &loc) {
        loc.visit([that=this, idx](const auto& val) {
            if(val)
                that->set_scalar(idx, val.value());
        });
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_scalar(position_t idx, T val) {
        ARCTICDB_TRACE(log::version(), "Segment setting scalar {} at row {} column {}", val, row_id_ + 1, idx);
        column(idx).set_scalar(row_id_ + 1, val);
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_external_block(position_t idx, T *val, size_t size) {
        column_unchecked(idx).set_external_block(row_id_ + 1, val, size);
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_sparse_block(position_t idx, T *val,  size_t rows_to_write) {
        column_unchecked(idx).set_sparse_block(row_id_ + 1, val, rows_to_write);
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset) {
        column_unchecked(idx).set_sparse_block(std::move(buffer), std::move(bitset));
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
        column_unchecked(idx).set_sparse_block(std::move(buffer), std::move(shapes), std::move(bitset));
    }

    template<class T>
    requires std::same_as<std::decay_t<T>, std::string>
    void set_scalar(position_t idx, const T& val) {
        set_string(idx, val);
    }

    template<class T, template<class> class Tensor>
    requires std::integral<T> || std::floating_point<T>
    void set_array(position_t pos, Tensor<T> &val) {
        magic_.check();
        ARCTICDB_SAMPLE(MemorySegmentSetArray, 0)
        column_unchecked(pos).set_array(row_id_ + 1, val);
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_array(position_t pos, py::array_t<T>& val) {
        magic_.check();
        ARCTICDB_SAMPLE(MemorySegmentSetArray, 0)
        column_unchecked(pos).set_array(row_id_ + 1, val);
    }

    void set_string(position_t pos, std::string_view str) {
        ARCTICDB_TRACE(log::version(), "Segment setting string {} at row {} column {}", str, row_id_ + 1, pos);
        OffsetString ofstr = string_pool_->get(str);
        column_unchecked(pos).set_scalar(row_id_ + 1, ofstr.offset());
    }

    void set_string_at(position_t col, position_t row, const char *str, size_t size) {
        OffsetString ofstr = string_pool_->get(str, size);
        column_unchecked(col).set_scalar(row, ofstr.offset());
    }

    void set_string_array(position_t idx, size_t string_size, size_t num_strings, char *data) {
        check_column_index(idx);
        column_unchecked(idx).set_string_array(row_id_ + 1, string_size, num_strings, data, string_pool());
    }

    void set_string_list(position_t idx, const std::vector<std::string> &input) {
        check_column_index(idx);
        column_unchecked(idx).set_string_list(row_id_ + 1, input, string_pool());
    }

    //pybind11 can't resolve const and non-const version of column()
    Column &column_ref(position_t idx) {
        return column(idx);
    }

    Column &column(position_t idx) {
        check_column_index(idx);
        return column_unchecked(idx);
    }

    const Column &column(position_t idx) const {
        check_column_index(idx);
        return column_unchecked(idx);
    }

    Column &column_unchecked(position_t idx) {
        return *columns_[idx];
    }

    std::shared_ptr<Column> column_ptr(position_t idx) const {
        return columns_[idx];
    }

    const Column &column_unchecked(position_t idx) const {
        return *columns_[idx];
    }

    std::vector<std::shared_ptr<Column>> &columns() {
        return columns_;
    }

    const std::vector<std::shared_ptr<Column>> &columns() const {
        return columns_;
    }

    bool empty() const {
        return row_count() <= 0 && !metadata();
    }

    void unsparsify() const {
        for(const auto& column : columns_)
            column->unsparsify(row_count());
    }

    void sparsify() const {
        for(const auto& column : columns_)
            column->sparsify();
    }

    void append(const SegmentInMemoryImpl& other);

    void concatenate(SegmentInMemoryImpl&& other, bool unique_column_names);

    void sort(const std::string& column);
    void sort(position_t idx);
    void sort(const std::vector<std::string>& column_names);
    void sort(const std::vector<position_t>& columns);

    position_t add_column(const Field &field, const std::shared_ptr<Column>& column);

    position_t add_column(const Field &field, size_t num_rows, AllocationType presize);

    position_t add_column(FieldRef field, size_t num_rows, AllocationType presize);

    position_t add_column(FieldRef field_ref, const std::shared_ptr<Column>& column);

    void change_schema(StreamDescriptor descriptor);

    template<typename T>
    std::optional<T> scalar_at(position_t row, position_t col) const {
        util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in scalar", row);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(!is_empty_type(column(col).type().data_type()),
                                                        "scalar_at called with empty-type column");
        return column(col).scalar_at<T>(row);
    }

    bool has_value_at(position_t row, position_t col) const {
        return column(col).has_value_at(row);
    }

    template<typename T>
    T &reference_at(position_t row, position_t col) {
        util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in scalar ref", row);
        return column(col).reference_at<T>(row);
    }

    template<typename T>
    std::optional<TensorType<T>> tensor_at(position_t row, position_t col) const {
        util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in tensor", row);
        return column(col).tensor_at<T>(row);
    }

    std::optional<std::string_view> string_at(position_t row, position_t col) const;

    std::optional<Column::StringArrayData> string_array_at(position_t row, position_t col);

    size_t num_blocks() const;

    size_t num_bytes() const;

    size_t num_columns() const { return columns_.size(); }

    size_t num_fields() const { return descriptor().field_count(); }

    size_t row_count() const { return row_id_ + 1 < 0 ? 0 : size_t(row_id_ + 1); }

    void clear() {
        columns_.clear();
        string_pool_->clear();
    }

    size_t string_pool_size() const { return string_pool_->size(); }

    bool has_string_pool() const { return string_pool_size() > 0; }

    const std::shared_ptr<StringPool>& string_pool_ptr() const {
        return string_pool_;
    }

    void check_column_index(position_t idx) const {
        util::check_arg(idx < position_t(columns_.size()), "Column index {} out of bounds", idx);
    }

    void init_column_map() const;

    ColumnData string_pool_data() const {
        return ColumnData{
            &string_pool_->data(),
            &string_pool_->shapes(),
            string_pool_descriptor().type(),
            nullptr
        };
    }

    void compact_blocks() const {
        for(const auto& column : columns_)
            column->compact_blocks();
    }

    const auto &fields() const {
        return descriptor().fields();
    }

    ColumnData column_data(size_t col) const {
        return columns_[col]->data();
    }

    const StreamDescriptor &descriptor() const {
        return *descriptor_;
    }

    StreamDescriptor &descriptor() {
        return *descriptor_;
    }

    const std::shared_ptr<StreamDescriptor>& descriptor_ptr() const {
        util::check(static_cast<bool>(descriptor_), "Descriptor pointer is null");
        return descriptor_;
    }

    void attach_descriptor(std::shared_ptr<StreamDescriptor> desc) {
        descriptor_ = std::move(desc);
    }

    void drop_column(std::string_view name);

    const Field& field(size_t index) const {
        return descriptor()[index];
    }

    void set_row_id(ssize_t rid) {
        row_id_ = rid;
    }

    void set_row_data(ssize_t rid) {
        set_row_id(rid);
        for(const auto& column : columns())
            column->set_row_data(row_id_);
    }

    StringPool &string_pool() { return *string_pool_; } //TODO protected

    void reset_metadata();

    void set_metadata(google::protobuf::Any&& meta);

    bool has_metadata() const;

    const google::protobuf::Any* metadata() const;

    bool is_index_sorted() const;

    bool compacted() const {
        return compacted_;
    }

    void set_compacted(bool value) {
        compacted_ = value;
    }

    void check_magic() const {
        magic_.check();
    }

    friend bool operator==(const SegmentInMemoryImpl& left, const SegmentInMemoryImpl& right);

    bool allow_sparse() const{
        return allow_sparse_ == Sparsity::PERMITTED;
    }

    // TODO: Very slow, fix this by storing it in protobuf
    bool is_sparse() const {
        return std::any_of(std::begin(columns_), std::end(columns_), [] (const auto& c) {
            return c->is_sparse();
        });
    }

    SegmentInMemoryImpl clone() const;

    void set_string_pool(std::shared_ptr<StringPool> string_pool) {
        string_pool_ = std::move(string_pool);
    }

    std::shared_ptr<SegmentInMemoryImpl> get_output_segment(size_t num_values, bool pre_allocate=true) const;

    std::shared_ptr<SegmentInMemoryImpl> filter(util::BitSet&& filter_bitset,
                                                bool filter_down_stringpool=false,
                                                bool validate=false) const;

    bool has_index_descriptor() const {
        return tsd_.has_value();
    }

    void set_timeseries_descriptor(const TimeseriesDescriptor& tsd);

    void reset_timeseries_descriptor();

    void calculate_statistics();

    bool has_user_metadata() {
        return tsd_.has_value() && !tsd_->proto_is_null() && tsd_->proto().has_user_meta();
    }

    const arcticdb::proto::descriptors::UserDefinedMetadata& user_metadata() const {
        return tsd_->user_metadata();
    }

    /// @brief Construct a copy of the segment containing only rows in [start_row; end_row)
    /// @param start_row Start of the row range (inclusive)
    /// @param end_Row End of the row range (exclusive)
    /// @param reconstruct_string_pool When truncating some of the strings values of the original
    ///  segment might not be referenced in the resulting segment. In this case, reconstructing the
    ///  string pool will save some memory. Note that reconstructing the string pool is an expensive
    ///  operation and should be avoided if possible.
    std::shared_ptr<SegmentInMemoryImpl> truncate(
        size_t start_row,
        size_t end_row,
        bool reconstruct_string_pool
    ) const;

    // Partitions the segment into n new segments. Each row in the starting segment is mapped to one of the output segments
    // by the row_to_segment vector (std::nullopt means the row is not included in any output segment).
    // segment_counts is the length of the number of output segments, and should be greater than or equal to the max value
    // in row_to_segment
    std::vector<std::shared_ptr<SegmentInMemoryImpl>> partition(const std::vector<uint8_t>& row_to_segment,
                                                       const std::vector<uint64_t>& segment_counts) const;

    std::vector<std::shared_ptr<SegmentInMemoryImpl>> split(size_t rows, bool filter_down_stringpool=false) const;
    void drop_empty_columns();

private:
    ssize_t row_id_ = -1;
    std::shared_ptr<StreamDescriptor> descriptor_ = std::make_shared<StreamDescriptor>();
    std::vector<std::shared_ptr<Column>> columns_;
    std::shared_ptr<StringPool> string_pool_ = std::make_shared<StringPool>();
    ssize_t offset_ = 0u;
    std::unique_ptr<google::protobuf::Any> metadata_;
    mutable std::shared_ptr<ColumnMap> column_map_;
    mutable std::unique_ptr<std::mutex> column_map_mutex_ = std::make_unique<std::mutex>();
    Sparsity allow_sparse_ = Sparsity::NOT_PERMITTED;
    bool compacted_ = false;
    util::MagicNum<'M', 'S', 'e', 'g'> magic_;
    std::optional<TimeseriesDescriptor> tsd_;
};

namespace {
inline std::shared_ptr<SegmentInMemoryImpl> allocate_sparse_segment(const StreamId& id, const IndexDescriptorImpl& index) {
    return std::make_shared<SegmentInMemoryImpl>(StreamDescriptor{id, index}, 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
}

inline std::shared_ptr<SegmentInMemoryImpl> allocate_dense_segment(const StreamDescriptor& descriptor, size_t row_count) {
    return std::make_shared<SegmentInMemoryImpl>(descriptor, row_count, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
}
} // namespace anon

} // namespace arcticdb
