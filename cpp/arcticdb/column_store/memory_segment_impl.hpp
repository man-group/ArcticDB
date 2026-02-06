/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/constructors.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <arcticdb/column_store/concepts.hpp>

namespace arcticdb {

class ColumnMap;

class SegmentInMemoryImpl {
  public:
    struct Location {
        Location(SegmentInMemoryImpl* parent, ssize_t row_id_, size_t column_id) :
            parent_(parent),
            row_id_(row_id_),
            column_id_(column_id) {}

        template<class Callable>
        auto visit(Callable&& c) const {
            return entity::visit_field(
                    parent_->descriptor().field(column_id_), [this, c = std::forward<Callable>(c)](auto type_desc_tag) {
                        using RawType = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag::raw_type;
                        return c(parent_->scalar_at<RawType>(row_id_, column_id_));
                    }
            );
        }

        template<class Callable>
        auto visit_field(Callable&& c) const {
            const auto& field = parent_->descriptor().field(column_id_);
            return entity::visit_field(field, [&field, this, c = std::forward<Callable>(c)](auto type_desc_tag) {
                using DataTypeTag = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag;
                using RawType = typename DataTypeTag::raw_type;
                if constexpr (is_sequence_type(DataTypeTag::data_type))
                    return c(
                            parent_->string_at(row_id_, position_t(column_id_)),
                            std::string_view{field.name()},
                            type_desc_tag
                    );
                else if constexpr (is_numeric_type(DataTypeTag::data_type) || is_bool_type(DataTypeTag::data_type))
                    return c(
                            parent_->scalar_at<RawType>(row_id_, column_id_),
                            std::string_view{field.name()},
                            type_desc_tag
                    );
                else if constexpr (is_empty_type(DataTypeTag::data_type))
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("visit_field does not support empty-type columns");
                else
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("visit_field called with unexpected column type");
            });
        }

        template<class Callable>
        auto visit(Callable&& c) {
            return entity::visit_field(
                    parent_->descriptor().field(column_id_), [this, c = std::forward<Callable>(c)](auto type_desc_tag) {
                        using RawType = typename std::decay_t<decltype(type_desc_tag)>::DataTypeTag::raw_type;
                        return c(parent_->reference_at<RawType>(row_id_, column_id_));
                    }
            );
        }

        [[nodiscard]] bool has_value() const;

        template<typename RawType>
        RawType& value() {
            return parent_->reference_at<RawType>(row_id_, column_id_);
        }

        template<typename RawType>
        [[nodiscard]] const RawType& value() const {
            return parent_->reference_at<RawType>(row_id_, column_id_);
        }

        bool operator==(const Location& other) const;

        SegmentInMemoryImpl* const parent_;
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
        RowIterator(SegmentInMemoryImpl* parent, ssize_t row_id_, size_t column_id) :
            location_(parent, row_id_, column_id) {}

        RowIterator(SegmentInMemoryImpl* parent, ssize_t row_id_) : location_(parent, row_id_, 0) {}

        template<class OtherValue>
        explicit RowIterator(const RowIterator<OtherValue>& other) : location_(other.location_) {}

      private:
        friend class boost::iterator_core_access;

        template<class>
        friend class SegmentRowIterator;

        template<class OtherValue>
        bool equal(const RowIterator<OtherValue>& other) const {
            return location_ == other.location_;
        }

        void increment() { ++location_.column_id_; }

        void decrement() { --location_.column_id_; }

        void advance(ptrdiff_t n) { location_.column_id_ += n; }

        ValueType& dereference() const { return location_; }

        mutable Location location_;
    };

    struct Row {
        using iterator = RowIterator<Location>;
        using const_iterator = RowIterator<const Location>;

        Row() = default;

        ~Row() = default;
        Row(const Row& other) = default;

        Row(SegmentInMemoryImpl* parent, ssize_t row_id_);

        [[nodiscard]] SegmentInMemoryImpl& segment() const;

        [[nodiscard]] const StreamDescriptor& descriptor() const;

        bool operator<(const Row& other) const;

        [[nodiscard]] size_t row_pos() const;

        template<class IndexType>
        [[nodiscard]] auto index() const {
            using RawType = typename IndexType::TypeDescTag::DataTypeTag::raw_type;
            return parent_->scalar_at<RawType>(row_id_, 0).value();
        }

        friend void swap(Row& left, Row& right) noexcept;

        Row& operator=(const Row& other) = default;

        Location operator[](int pos) const;

        iterator begin();

        iterator end();

        [[nodiscard]] const_iterator begin() const;

        [[nodiscard]] const_iterator end() const;

        bool operator==(const Row& other) const;

        void swap_parent(const Row& other);

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
                    } else if constexpr (is_numeric_type(T::DataTypeTag::data_type) ||
                                         is_bool_type(T::DataTypeTag::data_type)) {
                        if constexpr (std::is_same_v<RawType, S>) {
                            val = parent_->scalar_at<RawType>(row_id_, col);
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Type mismatch in scalar access");
                        }
                    } else if constexpr (is_empty_type(T::DataTypeTag::data_type)) {
                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                "scalar_at not supported with empty-type columns"
                        );
                    } else {
                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected data type in scalar access");
                    }
                } else {
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Scalar method called on multidimensional column");
                }
            });
            return val;
        }

        [[nodiscard]] std::optional<std::string_view> string_at(std::size_t col) const;

        SegmentInMemoryImpl* parent_;
        ssize_t row_id_;
    };

    template<class ValueType>
    class SegmentRowIterator
        : public boost::iterator_facade<SegmentRowIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
      public:
        using value_type = ValueType;

        explicit SegmentRowIterator(SegmentInMemoryImpl* parent) : row_(parent, 0) {}

        ~SegmentRowIterator() = default;

        SegmentRowIterator(const SegmentRowIterator& other) = default;

        SegmentRowIterator& operator=(const SegmentRowIterator& other) {
            row_.swap_parent(other.row_);
            row_.row_id_ = other.row_.row_id_;
            return *this;
        }

        SegmentRowIterator(SegmentInMemoryImpl* parent, ssize_t row_id_) : row_(parent, row_id_) {}

        template<class OtherValue>
        explicit SegmentRowIterator(const SegmentRowIterator<OtherValue>& other) : row_(other.row_) {}

      private:
        friend class boost::iterator_core_access;

        template<class>
        friend class SegmentRowIterator;

        template<class OtherValue>
        bool equal(const SegmentRowIterator<OtherValue>& other) const {
            return row_ == other.row_;
        }

        std::ptrdiff_t distance_to(const SegmentRowIterator& other) const { return other.row_.row_id_ - row_.row_id_; }

        void increment() { ++row_.row_id_; }

        void decrement() { --row_.row_id_; }

        void advance(ptrdiff_t n) { row_.row_id_ += n; }

        ValueType& dereference() const { return row_; }

        mutable Row row_;
    };

    using iterator = SegmentRowIterator<Row>;
    using const_iterator = SegmentRowIterator<const Row>;

    using ExtraBytesPerColumn = std::optional<std::vector<size_t>>;

    SegmentInMemoryImpl();

    SegmentInMemoryImpl(
            const StreamDescriptor& desc, size_t expected_column_size, AllocationType allocation_type,
            Sparsity allow_sparse, const ExtraBytesPerColumn& extra_bytes_per_column
    );

    ~SegmentInMemoryImpl();

    iterator begin();

    iterator end();

    const_iterator begin() const;

    const_iterator end() const;

    ARCTICDB_MOVE_ONLY_DEFAULT_EXCEPT(SegmentInMemoryImpl)

    void generate_column_map() const;

    void create_columns(
            size_t old_size, size_t expected_column_size, AllocationType allocation_type, Sparsity allow_sparse,
            const ExtraBytesPerColumn& extra_bytes_per_column = std::nullopt
    );

    size_t on_descriptor_change(
            const StreamDescriptor& descriptor, size_t expected_column_size, AllocationType allocation_type,
            Sparsity allow_sparse, const ExtraBytesPerColumn& extra_bytes_per_column = std::nullopt
    );

    std::optional<std::size_t> column_index(std::string_view name) const;

    [[nodiscard]] std::optional<std::size_t> column_index_with_name_demangling(std::string_view name) const;

    const Field& column_descriptor(size_t col);

    void end_row();

    const TimeseriesDescriptor& index_descriptor() const;

    TimeseriesDescriptor& mutable_index_descriptor();

    void end_block_write(ssize_t size);

    void set_offset(ssize_t offset);

    ssize_t offset() const;

    void push_back(const Row& row);

    void set_value(position_t idx, const Location& loc);

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_scalar(position_t idx, T val) {
        ARCTICDB_TRACE(log::version(), "Segment setting scalar {} at row {} column {}", val, row_id_ + 1, idx);
        column(idx).set_scalar(row_id_ + 1, val);
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_external_block(position_t idx, T* val, size_t size) {
        column_unchecked(idx).set_external_block(row_id_ + 1, val, size);
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_sparse_block(position_t idx, T* val, size_t rows_to_write) {
        column_unchecked(idx).set_sparse_block(row_id_ + 1, val, rows_to_write);
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset);

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset);

    template<class T>
    requires std::same_as<std::decay_t<T>, std::string>
    void set_scalar(position_t idx, const T& val) {
        set_string(idx, val);
    }

    template<util::arithmetic_tensor TensorType>
    void set_array(position_t pos, TensorType& val) {
        magic_.check();
        ARCTICDB_SAMPLE(MemorySegmentSetArray, 0)
        column_unchecked(pos).set_array(row_id_ + 1, val);
    }

    void set_string(position_t pos, std::string_view str);

    void set_string_at(position_t col, position_t row, const char* str, size_t size);

    void set_string_array(position_t idx, size_t string_size, size_t num_strings, char* data);

    void set_string_list(position_t idx, const std::vector<std::string>& input);

    // pybind11 can't resolve const and non-const version of column()
    Column& column_ref(position_t idx);

    Column& column(position_t idx);

    const Column& column(position_t idx) const;

    Column& column_unchecked(position_t idx);

    std::shared_ptr<Column> column_ptr(position_t idx) const;

    const Column& column_unchecked(position_t idx) const;

    std::vector<std::shared_ptr<Column>>& columns();

    const std::vector<std::shared_ptr<Column>>& columns() const;

    bool empty() const;

    void unsparsify() const;

    void sparsify() const;

    void append(const SegmentInMemoryImpl& other);

    void concatenate(SegmentInMemoryImpl&& other, bool unique_column_names);

    void sort(const std::string& column);
    void sort(position_t idx);
    void sort(const std::vector<std::string>& column_names);
    void sort(const std::vector<position_t>& columns);

    position_t add_column(const Field& field, const std::shared_ptr<Column>& column);

    position_t add_column(const Field& field, size_t num_rows, AllocationType presize);

    position_t add_column(FieldRef field, size_t num_rows, AllocationType presize);

    position_t add_column(std::string_view name, const std::shared_ptr<Column>& column);

    position_t add_column(FieldRef field_ref, const std::shared_ptr<Column>& column);

    void change_schema(StreamDescriptor descriptor);

    template<typename T>
    std::optional<T> scalar_at(position_t row, position_t col) const {
        util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in scalar", row);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                !is_empty_type(column(col).type().data_type()), "scalar_at called with empty-type column"
        );
        return column(col).scalar_at<T>(row);
    }

    bool has_value_at(position_t row, position_t col) const;

    template<typename T>
    T& reference_at(position_t row, position_t col) {
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

    size_t num_columns() const;

    size_t num_fields() const;

    size_t row_count() const;

    void clear();

    size_t string_pool_size() const;

    bool has_string_pool() const;

    const std::shared_ptr<StringPool>& string_pool_ptr() const;

    void check_column_index(position_t idx) const;

    void init_column_map() const;

    ColumnData string_pool_data() const;

    void compact_blocks() const;

    const FieldCollection& fields() const;

    ColumnData column_data(size_t col) const;

    const StreamDescriptor& descriptor() const;

    StreamDescriptor& descriptor();

    const std::shared_ptr<StreamDescriptor>& descriptor_ptr() const;

    void attach_descriptor(std::shared_ptr<StreamDescriptor> desc);

    void drop_column(std::string_view name);

    const Field& field(size_t index) const;

    void set_row_id(ssize_t rid);

    void set_row_data(ssize_t rid);

    StringPool& string_pool();

    void reset_metadata();

    void set_metadata(google::protobuf::Any&& meta);

    bool has_metadata() const;

    const google::protobuf::Any* metadata() const;

    bool is_index_sorted() const;

    bool compacted() const;

    void set_compacted(bool value);

    void check_magic() const;

    friend bool operator==(const SegmentInMemoryImpl& left, const SegmentInMemoryImpl& right);

    bool allow_sparse() const;

    // TODO: Very slow, fix this by storing it in protobuf
    bool is_sparse() const;

    SegmentInMemoryImpl clone() const;

    void set_string_pool(std::shared_ptr<StringPool> string_pool);

    std::shared_ptr<SegmentInMemoryImpl> get_output_segment(size_t num_values, bool pre_allocate = true) const;

    std::shared_ptr<SegmentInMemoryImpl> filter(
            util::BitSet&& filter_bitset, bool filter_down_stringpool = false, bool validate = false
    ) const;

    bool has_index_descriptor() const;

    void set_timeseries_descriptor(const TimeseriesDescriptor& tsd);

    void reset_timeseries_descriptor();

    void calculate_statistics();

    bool has_user_metadata();

    const arcticdb::proto::descriptors::UserDefinedMetadata& user_metadata() const;

    /// @brief Construct a copy of the segment containing only rows in [start_row; end_row)
    /// @param start_row Start of the row range (inclusive)
    /// @param end_Row End of the row range (exclusive)
    /// @param reconstruct_string_pool When truncating some of the strings values of the original
    ///  segment might not be referenced in the resulting segment. In this case, reconstructing the
    ///  string pool will save some memory. Note that reconstructing the string pool is an expensive
    ///  operation and should be avoided if possible.
    std::shared_ptr<SegmentInMemoryImpl> truncate(size_t start_row, size_t end_row, bool reconstruct_string_pool) const;

    // Partitions the segment into n new segments. Each row in the starting segment is mapped to one of the output
    // segments by the row_to_segment vector (std::nullopt means the row is not included in any output segment).
    // segment_counts is the length of the number of output segments, and should be greater than or equal to the max
    // value in row_to_segment
    std::vector<std::shared_ptr<SegmentInMemoryImpl>> partition(
            const std::vector<uint8_t>& row_to_segment, const std::vector<uint64_t>& segment_counts
    ) const;

    std::vector<std::shared_ptr<SegmentInMemoryImpl>> split(size_t rows, bool filter_down_stringpool = false) const;
    void drop_empty_columns();
    std::string_view string_at_offset(const position_t offset_in_string_pool) const;

  private:
    ssize_t row_id_ = -1;
    std::shared_ptr<StreamDescriptor> descriptor_;
    std::vector<std::shared_ptr<Column>> columns_;
    std::shared_ptr<StringPool> string_pool_;
    ssize_t offset_ = 0u;
    std::unique_ptr<google::protobuf::Any> metadata_;
    mutable std::shared_ptr<ColumnMap> column_map_;
    mutable std::unique_ptr<std::mutex> column_map_mutex_ = std::make_unique<std::mutex>();
    Sparsity allow_sparse_ = Sparsity::NOT_PERMITTED;
    bool compacted_ = false;
    util::MagicNum<'M', 'S', 'e', 'g'> magic_;
    std::optional<TimeseriesDescriptor> tsd_;
};

} // namespace arcticdb
