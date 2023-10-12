/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/cursor.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/column_store/column_map.hpp>
#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/util/third_party/emilib_map.hpp>
#include <arcticdb/util/format_date.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>

#include <google/protobuf/message.h>
#include <google/protobuf/any.h>
#include <google/protobuf/any.pb.h>
#include <boost/iterator/iterator_facade.hpp>
#include <folly/container/Enumerate.h>

namespace arcticdb {

class SegmentInMemoryImpl;

namespace {
inline std::shared_ptr<SegmentInMemoryImpl> allocate_sparse_segment(const StreamId& id, const IndexDescriptor& index);

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
} // namespace anon

inline bool operator==(const Field::Proto& left, const Field::Proto& right) {
    google::protobuf::util::MessageDifferencer diff;
    return diff.Compare(left, right);
}

inline bool operator<(const Field::Proto& left, const Field::Proto& right) {
    return left.name() < right.name();
}

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
            return entity::visit_field(parent_->descriptor().field(column_id_), [that=this, c=std::forward<Callable>(c)](auto type_desc_tag) {
                using RawType = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag::raw_type;
                return c(that->parent_->scalar_at<RawType>(that->row_id_, that->column_id_));
            });
        }

        template<class Callable>
        auto visit_string(Callable &&c) const {
            return entity::visit_field(parent_->descriptor().field(column_id_), [that=this, c = std::forward<Callable>(c)](auto type_desc_tag) {
                using DTT = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag;
                if constexpr(is_sequence_type(DTT::data_type))
                    return c(that->parent_->string_at(that->row_id_, position_t(that->column_id_)));
            });
        }

        template<class Callable>
        auto visit_field(Callable &&c) const {
            const auto& field = parent_->descriptor().field(column_id_);
            return entity::visit_field(parent_->descriptor().field(column_id_), [&field, that=this, c = std::forward<Callable>(c)](auto type_desc_tag) {
                using DataTypeTag = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag;
                using RawType = typename DataTypeTag::raw_type;
                if constexpr (is_sequence_type(DataTypeTag::data_type))
                    return c(that->parent_->string_at(that->row_id_, position_t(that->column_id_)), std::string_view{field.name()}, field.type());
                else if constexpr (is_numeric_type(DataTypeTag::data_type) || is_bool_type(DataTypeTag::data_type))
                    return c(that->parent_->scalar_at<RawType>(that->row_id_, that->column_id_), std::string_view{field.name()}, field.type());
                else if constexpr(is_empty_type(DataTypeTag::data_type))
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("visit_field does not support empty-type columns");
                else
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("visit_field called with unexpected column type");
            });
        }

        template<class Callable>
        auto visit(Callable &&c) {
            return entity::visit_field(parent_->descriptor().field(column_id_), [that=this, c=std::forward<Callable>(c)](auto type_desc_tag) {
                using RawType = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag::raw_type;
                return c(that->parent_->reference_at<RawType>(that->row_id_, that->column_id_));
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

        [[nodiscard]] auto& get_field() const {
            return parent_->descriptor().field(column_id_);
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

        [[nodiscard]] size_t col_count() const { return parent_->num_columns(); }

        [[nodiscard]] size_t row_pos() const { return row_id_; }

        template<class IndexType>
            auto index() {
            using RawType =  typename IndexType::TypeDescTag::DataTypeTag::raw_type;
            return parent_->scalar_at<RawType>(row_id_, 0).value();
        }

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

        void assert_same_parent(const Row &other) const {
            util::check(parent_ == other.parent_, "Invalid iterator comparison, different parents");
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



    SegmentInMemoryImpl() = default;

    explicit SegmentInMemoryImpl(
        const StreamDescriptor &desc,
        size_t expected_column_size,
        bool presize,
        bool allow_sparse) :
        descriptor_(std::make_shared<StreamDescriptor>(StreamDescriptor{desc.id(), desc.index()})),
        allow_sparse_(allow_sparse) {
        on_descriptor_change(desc, expected_column_size, presize, allow_sparse);
    }

    ~SegmentInMemoryImpl() {
        ARCTICDB_TRACE(log::version(), "Destroying segment in memory");
    }

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

    void generate_column_map() const {
        if (column_map_) {
            column_map_->clear();
            column_map_->set_from_descriptor(*descriptor_);
        }
    }

    void create_columns(size_t old_size, size_t expected_column_size, bool presize, bool allow_sparse) {
        columns_.reserve(descriptor_->field_count());
        for (size_t i = old_size; i < size_t(descriptor_->field_count()); ++i) {
            auto type = descriptor_->fields(i).type();
            util::check(type.data_type() != DataType::UNKNOWN, "Can't create column in create_columns with unknown data type");
            columns_.emplace_back(
                std::make_shared<Column>(descriptor_->fields(i).type(), expected_column_size, presize, allow_sparse));
        }
        generate_column_map();
    }

    /**
     * @param descriptor
     * @return false is descriptor change is not compatible and should trigger a segment commit
     */
    size_t on_descriptor_change(const StreamDescriptor &descriptor, size_t expected_column_size, bool presize, bool allow_sparse) {
        ARCTICDB_TRACE(log::storage(), "Entering descriptor change: descriptor is currently {}, incoming descriptor '{}'",
                      *descriptor_, descriptor);

        std::size_t old_size = descriptor_->fields().size();
        *descriptor_ = descriptor;
        create_columns(old_size, expected_column_size, presize, allow_sparse);
        ARCTICDB_TRACE(log::storage(), "Descriptor change: descriptor is now {}", *descriptor_);
        return old_size;
    }

    std::optional<std::size_t> column_index(std::string_view name) const {
        util::check(!name.empty(), "Cannot get index of empty column name");
        util::check(static_cast<bool>(column_map_), "Uninitialized column map");
        return column_map_->column_index(name);
    }

    const Field& column_descriptor(size_t col) {
        return (*descriptor_)[col];
    }

    void end_row() {
        row_id_++;
    }

    std::shared_ptr<FieldCollection> index_fields() const {
        return index_fields_;
    }

    void set_index_fields(std::shared_ptr<FieldCollection> index_fields) {
        index_fields_ = std::move(index_fields);
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

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
        void set_scalar(position_t idx, T val) {
        ARCTICDB_TRACE(log::version(), "Segment setting scalar {} at row {} column {}", val, row_id_ + 1, idx);
        column(idx).set_scalar(row_id_ + 1, val);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
        void set_external_block(position_t idx, T *val, size_t size) {
        column_unchecked(idx).set_external_block(row_id_ + 1, val, size);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_sparse_block(position_t idx, T *val,  size_t rows_to_write) {
        column_unchecked(idx).set_sparse_block(row_id_ + 1, val, rows_to_write);
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset) {
        column_unchecked(idx).set_sparse_block(std::move(buffer), std::move(bitset));
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
        column_unchecked(idx).set_sparse_block(std::move(buffer), std::move(shapes), std::move(bitset));
    }

    void set_secondary_type(position_t idx, TypeDescriptor type) {
        column_unchecked(idx).set_secondary_type(type);
    }

    template<class T, std::enable_if_t<std::is_same_v<std::decay_t<T>, std::string>, int> = 0>
        void set_scalar(position_t idx, T val) {
        set_string(idx, val);
    }

    template<class T, template<class> class Tensor, std::enable_if_t<
        std::is_integral_v<T> || std::is_floating_point_v<T>,
        int> = 0>
            void set_array(position_t pos, Tensor<T> &val) {
        magic_.check();
        ARCTICDB_SAMPLE(MemorySegmentSetArray, 0)
        column_unchecked(pos).set_array(row_id_ + 1, val);
    }

    template<class T, std::enable_if_t<
        std::is_integral_v<T> || std::is_floating_point_v<T>,
        int> = 0>
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

    void set_no_string_at(position_t col, position_t row, OffsetString::offset_t placeholder) {
        column_unchecked(col).set_scalar(row, placeholder);
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

    std::shared_ptr<Column> column_ptr(position_t idx) {
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

    util::BitSet get_duplicates_bitset(SegmentInMemoryImpl& other);
    bool is_row_duplicate(const SegmentInMemoryImpl::Row& row);

    void sort(const std::string& column);
    void sort(position_t idx);



    position_t add_column(const Field &field, const std::shared_ptr<Column>& column);

    position_t add_column(const Field &field, size_t num_rows, bool presize);

    position_t add_column(FieldRef field, size_t num_rows, bool presize);

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

    size_t row_count() const { return size_t(row_id_ + 1); }

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

    void init_column_map() const {
        std::lock_guard lock{*column_map_mutex_};
        if(column_map_)
            return;

        column_map_ = std::make_shared<ColumnMap>(descriptor().field_count());
        column_map_->set_from_descriptor(descriptor());
    }

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

    void drop_column(std::string_view name) {
        std::lock_guard lock(*column_map_mutex_);
        auto opt_column_index = column_index(name);
        internal::check<ErrorCode::E_INVALID_ARGUMENT>(opt_column_index.has_value(), "Cannot drop column with name '{}' as it doesn't exist", name);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(*opt_column_index < columns_.size(), "Column index out of range in drop_column");
        auto it = std::begin(columns_);
        std::advance(it, *opt_column_index);
        columns_.erase(it);
        descriptor_->erase_field(*opt_column_index);
        column_map_->erase(name);
    }

    const Field& field(size_t index) const {
        return descriptor()[index];
    }

    void set_row_id(ssize_t rid) {
        row_id_ = rid;
    }

    ssize_t get_row_id() {
        return row_id_;
    }

    void set_row_data(ssize_t rid) {
        set_row_id(rid);
        for(const auto& column : columns())
            column->set_row_data(row_id_);
    }

    void string_pool_assign(const SegmentInMemoryImpl& other) {
        string_pool_ = other.string_pool_;
    }

    StringPool &string_pool() { return *string_pool_; } //TODO protected

    void set_metadata(google::protobuf::Any &&meta) {
        util::check_arg(!metadata_, "Cannot override previously set metadata");
        if (meta.ByteSizeLong())
            metadata_ = std::make_unique<google::protobuf::Any>(std::move(meta));
    }

    void override_metadata(google::protobuf::Any &&meta) {
        if (meta.ByteSizeLong())
            metadata_ = std::make_unique<google::protobuf::Any>(std::move(meta));
    }

    bool has_metadata() {
        return static_cast<bool>(metadata_);
    }

    const google::protobuf::Any *metadata() const {
        return metadata_.get();
    }

    bool is_index_sorted() const {
        timestamp idx = 0;
        for (auto it = begin(); it != end(); ++it) {
            auto val = it->begin()->value<timestamp>();
            if (idx > val)
                return false;

            idx = val;
        }
        return true;
    }

    bool compacted() const {
        return compacted_;
    }

    void set_compacted(bool value) {
        compacted_ = value;
    }

    void check_magic() const {
        magic_.check();
    }

    friend bool operator==(const SegmentInMemoryImpl& left, const SegmentInMemoryImpl& right) {
        if(*left.descriptor_ != *right.descriptor_ ||
        left.offset_ != right.offset_)
            return false;

        if(left.columns_.size() != right.columns_.size())
            return false;

        for(auto col = 0u; col < left.columns_.size(); ++col) {
            if (is_sequence_type(left.column(col).type().data_type())) {
                const auto& left_col = left.column(col);
                const auto& right_col = right.column(col);
                if(left_col.type() != right_col.type())
                    return false;

                if(left_col.row_count() != right_col.row_count())
                    return false;

                for(auto row = 0u; row < left_col.row_count(); ++row)
                    if(left.string_at(row, col) != right.string_at(row, col))
                        return false;
            } else if (is_numeric_type(left.column(col).type().data_type()) || is_bool_type(left.column(col).type().data_type())) {
                if (left.column(col) != right.column(col))
                    return false;
            } else if (is_empty_type(left.column(col).type().data_type())) {
                if (!is_empty_type(right.column(col).type().data_type()))
                    return false;
            } else {
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected data type in SegmentInMemory equality check");
            }
        }
        return true;
    }

    bool allow_sparse() const{
        return allow_sparse_;
    }

    // TODO: Very slow, fix this by storing it in protobuf
    bool is_sparse() const {
        return std::any_of(std::begin(columns_), std::end(columns_), [] (const auto& c) {
            return c->is_sparse();
        });
    }

    SegmentInMemoryImpl clone() const {
        SegmentInMemoryImpl output{};
        output.row_id_ = row_id_;
        *output.descriptor_ = descriptor_->clone();

        for(const auto& column : columns_) {
            output.columns_.push_back(std::make_shared<Column>(column->clone()));
        }

        output.string_pool_ = string_pool_->clone();
        output.offset_ = offset_;
        if(metadata_) {
            google::protobuf::Any metadata;
            metadata.CopyFrom(*metadata_);
            output.metadata_ = std::make_unique<google::protobuf::Any>(std::move(metadata));
        }
        output.allow_sparse_ = allow_sparse_;
        output.compacted_ = compacted_;
        return output;
    }

    void set_string_pool(const std::shared_ptr<StringPool>& string_pool) {
        string_pool_ = string_pool;
    }

    // This implementation of split is incorrect Column::split doesn't work for sparse columns
    // Will use filter_segment for now - which should do the same amount of copying as this implementation
    std::vector<std::shared_ptr<SegmentInMemoryImpl>> old_split(size_t rows) {
        std::vector<std::shared_ptr<SegmentInMemoryImpl>> output;
        const auto output_size = std::ceil(double(row_count()) / double(rows));
        for(auto i = 0u; i < output_size; ++i) {
            output.push_back(std::make_shared<SegmentInMemoryImpl>());
            if(has_string_pool())
                (*output.rbegin())->set_string_pool(string_pool_);
        }

        for(const auto& column : folly::enumerate(columns_)) {
            auto split_columns = Column::split(*column, rows);
            for(auto split_col : folly::enumerate(split_columns)) {
                output[split_col.index]->add_column(descriptor_->field(column.index), *split_col);
            }
        }

        auto remaining_rows = row_count();
        for(const auto& seg : output) {
            const auto current_rows = std::min(remaining_rows, rows);
            seg->set_row_data(static_cast<ssize_t>(current_rows) - 1);
            remaining_rows -= current_rows;
        }
        util::check(remaining_rows == 0, "Didn't manage to set the rows correctly: {} rows left", remaining_rows);

        return output;
    }

    std::shared_ptr<SegmentInMemoryImpl> get_output_segment(size_t num_values, bool pre_allocate=true) const {
        std::shared_ptr<SegmentInMemoryImpl> output;
        if (is_sparse()) {
            output = allocate_sparse_segment(descriptor().id(), descriptor().index());
        } else {
            if (pre_allocate)
                output = allocate_dense_segment(descriptor(), num_values);
            else
                output = allocate_dense_segment(StreamDescriptor(descriptor().id(), descriptor().index(), {}), num_values);
        }
        return output;
    }

    inline std::shared_ptr<SegmentInMemoryImpl> filter(const util::BitSet& filter_bitset,
                                                       bool filter_down_stringpool=false,
                                                       bool validate=false) const {
        bool is_input_sparse = is_sparse();
        util::check(is_input_sparse || row_count() == filter_bitset.size(), "Filter input sizes do not match: {} != {}", row_count(), filter_bitset.size());
        auto num_values = filter_bitset.count();
        if(num_values == 0)
            return std::shared_ptr<SegmentInMemoryImpl>{};

        auto output = get_output_segment(num_values);
        auto output_string_pool = filter_down_stringpool ? std::make_shared<StringPool>() : string_pool_;
        // Map from offsets in the input stringpool to offsets in the output stringpool
        // Only used if filter_down_stringpool is true
        emilib::HashMap<StringPool::offset_t, StringPool::offset_t> input_to_output_offsets;
        // Prepopulate with None and NaN placeholder values to avoid an if statement in a tight loop later
        input_to_output_offsets.insert(not_a_string(), not_a_string());
        input_to_output_offsets.insert(nan_placeholder(), nan_placeholder());

        // Index is built to make rank queries faster
        std::unique_ptr<util::BitIndex> filter_idx;
        for(const auto& column : folly::enumerate(columns())) {
            (*column)->type().visit_tag([&] (auto type_desc_tag){
                using TypeDescriptorTag =  decltype(type_desc_tag);
                using DataTypeTag = typename TypeDescriptorTag::DataTypeTag;
                using RawType = typename DataTypeTag::raw_type;
                const util::BitSet* final_bitset;
                util::BitSet bitset_including_sparse;

                auto sparse_map = (*column)->opt_sparse_map();
                std::unique_ptr<util::BitIndex> sparse_idx;
                auto output_col_idx = column.index;
                if (is_input_sparse || sparse_map) {
                    filter_idx = std::make_unique<util::BitIndex>();
                    filter_bitset.build_rs_index(filter_idx.get());
                    bitset_including_sparse = filter_bitset;
                    if (sparse_map) {
                        bitset_including_sparse = bitset_including_sparse.bit_and(sparse_map.value());
                        // Index is built to make rank queries faster
                        sparse_idx = std::make_unique<util::BitIndex>();
                        sparse_map.value().build_rs_index(sparse_idx.get());
                    } else {
                        bitset_including_sparse.resize((*column)->row_count());
                    }
                    if (bitset_including_sparse.count() == 0) {
                        // No values are set in the sparse column, skip it
                        return;
                    }
                    output_col_idx = output->add_column(field(column.index), bitset_including_sparse.count(), true);
                    final_bitset = &bitset_including_sparse;
                } else {
                    final_bitset = &filter_bitset;
                }
                auto& output_col = output->column(position_t(output_col_idx));
                if (sparse_map)
                    output_col.opt_sparse_map() = std::make_optional<util::BitSet>();
                auto output_ptr = reinterpret_cast<RawType*>(output_col.ptr());
                auto input_data =  (*column)->data();

                auto bitset_iter = final_bitset->first();
                auto row_count_so_far = 0;
                // Defines the position in output sparse column where we want to write data next (only used in sparse)
                // For dense, we just do +1
                util::BitSetSizeType pos_output = 0;
                while(auto block = input_data.next<TypeDescriptorTag>()) {
                    if (bitset_iter == final_bitset->end())
                        break;
                    auto input_ptr = block.value().data();
                    if (sparse_map) {
                        while(bitset_iter != final_bitset->end()) {
                            auto rank_in_filter = filter_bitset.rank(*bitset_iter, *filter_idx);
                            if (rank_in_filter - 1 != pos_output) {
                                // setting sparse_map - marking all rows in output as NULL until this point
                                output_col.mark_absent_rows(rank_in_filter - pos_output - 1);
                                pos_output = rank_in_filter - 1;
                            }
                            auto offset = sparse_map.value().rank(*bitset_iter, *sparse_idx) - row_count_so_far - 1;
                            auto value = *(input_ptr + offset);
                            if constexpr(is_sequence_type(DataTypeTag::data_type)) {
                                if (filter_down_stringpool) {
                                    if (auto it = input_to_output_offsets.find(value);
                                    it != input_to_output_offsets.end()) {
                                        *output_ptr = it->second;
                                    } else {
                                        auto str = string_pool_->get_const_view(value);
                                        auto output_string_pool_offset = output_string_pool->get(str, false).offset();
                                        *output_ptr = output_string_pool_offset;
                                        input_to_output_offsets.insert_unique(std::move(value), std::move(output_string_pool_offset));
                                    }
                                } else {
                                    *output_ptr = value;
                                }
                            } else if constexpr(is_numeric_type(DataTypeTag::data_type) || is_bool_type(DataTypeTag::data_type)){
                                *output_ptr = value;
                            } else if constexpr(is_empty_type(DataTypeTag::data_type)) {
                                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected block in empty type column in SegmentInMemoryImpl::filter");
                            } else {
                                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected column type in SegmentInMemoryImpl::filter");
                            }
                            output_ptr++;
                            output_col.opt_sparse_map().value()[pos_output++] = true;
                            bitset_iter++;
                        }
                    } else {
                        const auto row_count = block.value().row_count();
                        const auto bitset_end = final_bitset->end();
                        do {
                            const auto offset = *bitset_iter - row_count_so_far;
                            if (offset >= row_count)
                                break;

                            auto value = *(input_ptr + offset);
                            if constexpr(is_sequence_type(DataTypeTag::data_type)) {
                                if (filter_down_stringpool) {
                                    if (auto it = input_to_output_offsets.find(value);
                                    it != input_to_output_offsets.end()) {
                                        *output_ptr = it->second;
                                    } else {
                                        auto str = string_pool_->get_const_view(value);
                                        auto output_string_pool_offset = output_string_pool->get(str, false).offset();
                                        *output_ptr = output_string_pool_offset;
                                        input_to_output_offsets.insert_unique(std::move(value), std::move(output_string_pool_offset));
                                    }
                                } else {
                                    *output_ptr = value;
                                }
                            } else if constexpr(is_numeric_type(DataTypeTag::data_type) || is_bool_type(DataTypeTag::data_type)){
                                *output_ptr = value;
                            } else if constexpr(is_empty_type(DataTypeTag::data_type)) {
                                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected block in empty type column in SegmentInMemoryImpl::filter");
                            } else {
                                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected column type in SegmentInMemoryImpl::filter");
                            }

                            ++output_ptr;
                            bitset_iter.advance();
                        } while (bitset_iter < bitset_end);
                    }
                    row_count_so_far += block.value().row_count();
                }
                if (sparse_map && validate)
                    check_output_bitset(output_col.opt_sparse_map().value(), filter_bitset, sparse_map.value());
            });
        }
        if (num_values != 0)
            output->set_row_data(num_values - 1);

        output->set_string_pool(output_string_pool);
        output->set_compacted(compacted_);
        if (metadata_) {
            google::protobuf::Any metadata;
            metadata.CopyFrom(*metadata_);
            output->set_metadata(std::move(metadata));
        }
        return output;
    }

    std::shared_ptr<arcticdb::proto::descriptors::TimeSeriesDescriptor> timeseries_proto() {
        if(!tsd_) {
            tsd_ = std::make_shared<arcticdb::proto::descriptors::TimeSeriesDescriptor>();
            metadata_->UnpackTo(tsd_.get());
        }
        return tsd_;
    }

    TimeseriesDescriptor index_descriptor() {
        return {timeseries_proto(), index_fields_};
    }

    bool has_index_fields() const {
        return static_cast<bool>(index_fields_);
    }

    FieldCollection&& detach_index_fields() {
        return std::move(*index_fields_);
    }

    const FieldCollection& timeseries_fields() const {
        return *index_fields_;
    }

    void set_timeseries_descriptor(TimeseriesDescriptor&& tsd) {
        index_fields_ = tsd.fields_ptr();
        tsd_ = tsd.proto_ptr();
        util::check(!tsd_->has_stream_descriptor() || tsd_->stream_descriptor().has_index(), "Stream descriptor without index in set_timeseries_descriptor");
        google::protobuf::Any any;
        any.PackFrom(tsd.proto());
        set_metadata(std::move(any));
    }

    // Inclusive of start_row, exclusive of end_row
    inline std::shared_ptr<SegmentInMemoryImpl> truncate(
            size_t start_row,
            size_t end_row) const {
        auto num_values = end_row - start_row;
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                is_sparse() || (start_row < row_count() && end_row <= row_count() && num_values > 0),
                "Truncate bounds start_row={} end_row={} outside valid range {}", start_row, end_row, row_count());

        auto output = std::make_shared<SegmentInMemoryImpl>();

        output->set_row_data(num_values - 1);
        output->set_string_pool(string_pool_);
        output->set_compacted(compacted_);
        if (metadata_) {
            google::protobuf::Any metadata;
            metadata.CopyFrom(*metadata_);
            output->set_metadata(std::move(metadata));
        }

        for(const auto&& [idx, column] : folly::enumerate(columns_)) {
            auto truncated_column = Column::truncate(column, start_row, end_row);
            output->add_column(descriptor_->field(idx), truncated_column);
        }
        output->attach_descriptor(descriptor_);
        return output;
    }

    // Partitions the segment into n new segments. Each row in the starting segment is mapped to one of the output segments
    // by the row_to_segment vector (std::nullopt means the row is not included in any output segment).
    // segment_counts is the length of the number of output segments, and should be greater than or equal to the max value
    // in row_to_segment
    inline std::vector<std::shared_ptr<SegmentInMemoryImpl>> partition(const std::vector<std::optional<uint8_t>>& row_to_segment,
                                                       const std::vector<uint64_t>& segment_counts) const {
        schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(!is_sparse(),
                                                            "SegmentInMemory::partition not supported with sparse columns");
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(row_count() == row_to_segment.size(),
                    "row_to_segment size does not match segment row count: {} != {}", row_to_segment.size(), row_count());
        std::vector<std::shared_ptr<SegmentInMemoryImpl>> output(segment_counts.size());
        if(std::all_of(segment_counts.begin(), segment_counts.end(), [](const size_t& segment_count) { return segment_count == 0; })) {
            return output;
        }

        for (const auto& segment_count: folly::enumerate(segment_counts)) {
            if (*segment_count > 0) {
                auto& seg = output.at(segment_count.index);
                seg = get_output_segment(*segment_count);
                seg->set_row_data(*segment_count - 1);
                seg->set_string_pool(string_pool_);
                seg->set_compacted(compacted_);
                if (metadata_) {
                    google::protobuf::Any metadata;
                    metadata.CopyFrom(*metadata_);
                    seg->set_metadata(std::move(metadata));
                }
            }
        }

        for(const auto& column : folly::enumerate(columns())) {
            (*column)->type().visit_tag([&] (auto type_desc_tag){
                using TypeDescriptorTag = decltype(type_desc_tag);
                using ColumnTagType = typename TypeDescriptorTag::DataTypeTag;
                using RawType = typename ColumnTagType::raw_type;

                auto output_col_idx = column.index;
                std::vector<RawType*> output_ptrs{output.size(), nullptr};
                for (const auto& segment: folly::enumerate(output)) {
                    if (static_cast<bool>(*segment)) {
                        output_ptrs.at(segment.index) = reinterpret_cast<RawType*>((*segment)->column(output_col_idx).ptr());
                    }
                }

                auto input_data =  (*column)->data();
                size_t overall_idx = 0;
                while(auto block = input_data.next<TypeDescriptorTag>()) {
                    auto input_ptr = reinterpret_cast<const RawType*>(block.value().data());
                    for (size_t block_idx = 0; block_idx < block.value().row_count(); ++block_idx, ++input_ptr, ++overall_idx) {
                        auto opt_output_segment_idx = row_to_segment[overall_idx];
                        if (opt_output_segment_idx.has_value()) {
                            *(output_ptrs[*opt_output_segment_idx]++) = *input_ptr;
                        }
                    }
                }
            });
        }
        return output;
    }

    std::vector<std::shared_ptr<SegmentInMemoryImpl>> split(size_t rows) const{
        std::vector<std::shared_ptr<SegmentInMemoryImpl>> output;
        util::check(rows > 0, "rows supplied in SegmentInMemoryImpl.split() is non positive");
        auto total_rows = row_count();
        util::BitSetSizeType start = 0;
        for(; start < total_rows; start += rows) {
            util::BitSet bitset(total_rows);
            util::BitSetSizeType end = std::min(start + rows, total_rows);
            // set_range is close interval on [left, right]
            bitset.set_range(start, end - 1, true);
            output.emplace_back(filter(bitset));
        }
        return output;
    }

    StreamId get_index_col_name() const{
        return descriptor().id();
    }

private:
    ssize_t row_id_ = -1;
    std::shared_ptr<StreamDescriptor> descriptor_ = std::make_shared<StreamDescriptor>();
    std::vector<std::shared_ptr<Column>> columns_;
    std::shared_ptr<StringPool> string_pool_ = std::make_shared<StringPool>();
    ssize_t offset_ = 0u;
    std::unique_ptr<google::protobuf::Any> metadata_;
    mutable std::shared_ptr<ColumnMap> column_map_;
    mutable std::unique_ptr<std::mutex> column_map_mutex_ = std::make_unique<std::mutex>();
    bool allow_sparse_ = false;
    bool compacted_ = false;
    util::MagicNum<'M', 'S', 'e', 'g'> magic_;
    std::shared_ptr<FieldCollection> index_fields_;
    std::shared_ptr<arcticdb::proto::descriptors::TimeSeriesDescriptor> tsd_;
};

namespace {
inline std::shared_ptr<SegmentInMemoryImpl> allocate_sparse_segment(const StreamId& id, const IndexDescriptor& index) {
    return std::make_shared<SegmentInMemoryImpl>(StreamDescriptor{id, index}, 0, false, true);
}

inline std::shared_ptr<SegmentInMemoryImpl> allocate_dense_segment(const StreamDescriptor& descriptor, size_t row_count) {
    return std::make_shared<SegmentInMemoryImpl>(descriptor, row_count, true, false);
}
} // namespace anon

} // namespace arcticdb
