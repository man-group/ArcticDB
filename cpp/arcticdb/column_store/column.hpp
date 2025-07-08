/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/column_store/column_data_random_accessor.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/flatten_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/sparse_utils.hpp>

#include <folly/container/Enumerate.h>
// Compilation fails on Mac if cstdio is not included prior to folly/Function.h due to a missing definition of memalign in folly/Memory.h
#ifdef __APPLE__
#include <cstdio>
#endif
#include <folly/Function.h>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <concepts>
#include <numeric>
#include <optional>

namespace py = pybind11;

namespace arcticdb {

// this is needed to make templates of templates work
// since py::array_t has more than one template parameter
// (the rest are defaulted)
template< class T>
using py_array_t = py::array_t<T>;

using namespace arcticdb::entity;

// N.B. this will not catch all the things that C++ considers to be narrowing conversions, because
// it doesn't take into account integral promotion, however we don't care about that for the
// purpose for which it is used in this file.
template <typename SourceType, typename TargetType>
constexpr bool is_narrowing_conversion() {
    if(sizeof(TargetType) < sizeof(SourceType))
        return true;

    if(sizeof(SourceType) == sizeof(TargetType) && std::is_integral_v<TargetType> && std::is_unsigned_v<SourceType> && std::is_signed_v<TargetType>) {
        return true;
    }

    return false;
}

struct JiveTable {
    explicit JiveTable(size_t num_rows) :
        orig_pos_(num_rows),
        sorted_pos_(num_rows) {
    }

    std::vector<uint32_t> orig_pos_;
    std::vector<uint32_t> sorted_pos_;
};

enum class ExtraBufferType : uint8_t {
    OFFSET,
    STRING,
    ARRAY,
    BITMAP
};


// Specifies a way to index extra buffers.
// We can attach extra buffers to each offset and type. This is used for OutputFormat::ARROW to store the extra buffers
// required to construct a string buffer.
// For example if we have an arrow Column with 2 chunks containing the strings ["a", "bc"], ["ab", "ab"], we'd have:
// Column data in blocks:
// offset:0 -> [0, 1] (int32_t)
// offset:8 -> [0, 0] (int32_t)
// Extra buffers for the column:
// offset_bytes=0, type_=OFFSET -> [0, 1, 3] (int64_t)
// offset_bytes=8, type_=OFFSET -> [0, 2] (int64_t)
// offset_bytes=0, type_=STRING -> "abc"
// offset_bytes=8, type_=STRING -> "ab"
struct ExtraBufferIndex {
    size_t offset_bytes_;
    ExtraBufferType type_;
};

inline bool operator==(const ExtraBufferIndex& lhs, const ExtraBufferIndex& rhs) {
    return (lhs.offset_bytes_ == rhs.offset_bytes_) && (lhs.type_ == rhs.type_);
}

struct ExtraBufferIndexHash {
    std::size_t operator()(const ExtraBufferIndex& index) const {
        return folly::hash::hash_combine(index.offset_bytes_, index.type_);
    }
};

struct ExtraBufferContainer {
    mutable std::mutex mutex_;
    std::unordered_map<ExtraBufferIndex, ChunkedBuffer, ExtraBufferIndexHash> buffers_;

    ChunkedBuffer& create_buffer(size_t offset, ExtraBufferType type, size_t size, AllocationType allocation_type) {
        std::lock_guard lock(mutex_);
        auto inserted = buffers_.try_emplace(ExtraBufferIndex{offset, type}, ChunkedBuffer{size, allocation_type});
        util::check(inserted.second, "Failed to insert additional chunked buffer at position {}", offset);
        return inserted.first->second;
    }

    void set_buffer(size_t offset, ExtraBufferType type, ChunkedBuffer&& buffer) {
        std::lock_guard lock(mutex_);
        buffers_.try_emplace(ExtraBufferIndex{offset, type}, std::move(buffer));
    }

    ChunkedBuffer& get_buffer(size_t offset, ExtraBufferType type) const {
        std::lock_guard lock(mutex_);
        auto it = buffers_.find(ExtraBufferIndex{offset, type});
        util::check(it != buffers_.end(), "Failed to find additional chunked buffer at position {}", offset);
        return const_cast<ChunkedBuffer&>(it->second);
    }

    bool has_buffer(size_t offset, ExtraBufferType type) const {
        std::lock_guard lock(mutex_);
        auto it = buffers_.find(ExtraBufferIndex{offset, type});
        return it != buffers_.end();
    }
};


class Column;
class StringPool;

template <typename T>
JiveTable create_jive_table(const Column& col);

void initialise_output_column(const Column& input_column, Column& output_column);

void initialise_output_column(const Column& left_input_column, const Column& right_input_column, Column& output_column);

void initialise_output_bitset(const Column& input_column, bool sparse_missing_value_output, util::BitSet& output_bitset);

class Column {
public:
    template<typename TDT, typename ValueType>
    class TypedColumnIterator :  public boost::iterator_facade<TypedColumnIterator<TDT, ValueType>, ValueType, boost::random_access_traversal_tag> {
        using RawType =  std::decay_t<typename TDT::DataTypeTag::raw_type>;
        static constexpr size_t type_size = sizeof(RawType);

        ColumnData  parent_;
        std::optional<TypedBlockData<TDT>> block_;
        typename TypedBlockData<TDT>::template TypedColumnBlockIterator<const ValueType> block_pos_;
        typename TypedBlockData<TDT>::template TypedColumnBlockIterator<const ValueType> block_end_;

        void set_block_range() {
            if(block_) {
                block_pos_ = std::begin(*block_);
                block_end_ = std::end(*block_);
            }
        }

        void set_next_block() {
            if(auto block  = parent_.next<TDT>(); block)
                block_.emplace(std::move(*block));
            else
                block_ = std::nullopt;

            set_block_range();
        }

    public:
        TypedColumnIterator(const Column& col, bool begin) :
            parent_(col.data()),
            block_(begin ? parent_.next<TDT>() : std::nullopt) {
            if(begin)
                set_block_range();
        }


        template <class OtherValue>
        explicit TypedColumnIterator(const TypedColumnIterator<TDT, OtherValue>& other) :
            parent_(other.parent_),
            block_(other.block_),
            block_pos_(other.block_pos_),
            block_end_(other.block_end_){ }

        template <class OtherValue>
        bool equal(const TypedColumnIterator<TDT, OtherValue>& other) const{
            if(block_) {
                if(!other.block_)
                    return false;

                return *block_ == *other.block_ && block_pos_ == other.block_pos_;
            }
            return !other.block_;
        }

        ssize_t distance_to(const TypedColumnIterator& other) const {
            return other.get_offset() - get_offset();
        }

        void increment(){
            ++block_pos_;
            if(block_pos_ == block_end_)
                set_next_block();
        }

        void decrement(){
            if(!block_) {
                block_ = parent_.last<TDT>();
                if(block_) {
                    block_pos_ = block_->begin();
                    std::advance(block_pos_, block_->row_count() - 1);
                }
            } else {
                --block_pos_;
                if (block_pos_ < block_->begin()) {
                    auto offset = block_->offset() - type_size;
                    set_offset(offset);
                }
            }
        }

        [[nodiscard]] ssize_t get_offset() const {
            if(!block_)
                return parent_.buffer().bytes() / type_size;

            const auto off = block_->offset();
            const auto dist = std::distance(std::begin(*block_), block_pos_);
            return off + dist;
        }

        void set_offset(size_t offset) {
            const auto bytes = offset * type_size;
            auto block_and_offset = parent_.buffer().block_and_offset(bytes);
            block_ = ColumnData::make_typed_block<TDT>(block_and_offset.block_);
            block_pos_ = std::begin(*block_);
            std::advance(block_pos_, block_and_offset.offset_ / type_size);
            block_end_ = std::end(block_.value());
        }

        void advance(ptrdiff_t n){
            auto offset = get_offset();
            offset += n;
            set_offset(offset);
        }

        const ValueType& dereference() const {
            return *block_pos_;
        }
    };

    struct StringArrayData {
        ssize_t num_strings_;
        ssize_t string_size_;
        const char *data_;

        StringArrayData(ssize_t n, ssize_t s, const char *d) :
                num_strings_(n),
                string_size_(s),
                data_(d) {
        }
    };

    Column() : type_(null_type_descriptor()) {}

    Column(TypeDescriptor type) :
        Column(type, 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED) {
    }

    Column(TypeDescriptor type, Sparsity allow_sparse) :
        Column(type, 0, AllocationType::DYNAMIC, allow_sparse) {
    }

    Column(TypeDescriptor type, Sparsity allow_sparse, ChunkedBuffer&& buffer) :
        data_(std::move(buffer)),
        type_(type),
        allow_sparse_(allow_sparse) {
    }

    Column(TypeDescriptor type, Sparsity allow_sparse, ChunkedBuffer&& buffer, Buffer&& shapes) :
        data_(std::move(buffer)),
        shapes_(std::move(shapes)),
        type_(type),
        allow_sparse_(allow_sparse) {
    }

    Column(
        TypeDescriptor type,
        size_t expected_rows,
        AllocationType presize,
        Sparsity allow_sparse) :
            data_(expected_rows * entity::internal_data_type_size(type), presize),
            type_(type),
            allow_sparse_(allow_sparse) {
        ARCTICDB_TRACE(log::inmem(), "Creating column with descriptor {}", type);
    }

    Column(
        TypeDescriptor type,
        size_t expected_rows,
        AllocationType presize,
        Sparsity allow_sparse,
        OutputFormat output_format,
        DataTypeMode mode) :
        data_(expected_rows * entity::data_type_size(type, output_format, mode), presize),
        type_(type),
        allow_sparse_(allow_sparse) {
        ARCTICDB_TRACE(log::inmem(), "Creating column with descriptor {}", type);
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(Column)

    friend bool operator==(const Column& left, const Column& right);
    friend bool operator!=(const Column& left, const Column& right);

    Column clone() const;

    bool empty() const;

    bool is_sparse() const;

    bool sparse_permitted() const;

    void set_statistics(FieldStatsImpl stats) {
        stats_ = stats;
    }

    bool has_statistics() const {
        return stats_.set_;
    };

    FieldStatsImpl get_statistics() const  {
        return stats_;
    }

    void backfill_sparse_map(ssize_t to_row) {
        ARCTICDB_TRACE(log::version(), "Backfilling sparse map to position {}", to_row);
        // Initialise the optional to an empty bitset if it has not been created yet
        auto& bitset = sparse_map();
        if (to_row >= 0) {
            bitset.set_range(0, bv_size(to_row), true);
        }
    }

    [[nodiscard]] util::BitMagic& sparse_map();
    [[nodiscard]] const util::BitMagic& sparse_map() const;
    [[nodiscard]] std::optional<util::BitMagic>& opt_sparse_map();
    [[nodiscard]] std::optional<util::BitMagic> opt_sparse_map() const;

    template<typename TagType>
    auto begin() const {
            using RawType = typename TagType::DataTypeTag::raw_type;
            return TypedColumnIterator<TagType, const RawType>(*this, true);
    }

    template<typename TagType>
    auto end() const {
        using RawType = typename TagType::DataTypeTag::raw_type;
        return TypedColumnIterator<TagType, const RawType>(*this, false);
    }

    template<typename T>
    requires std::integral<T> || std::floating_point<T>
    void set_scalar(ssize_t row_offset, T val);

    template<class T>
    void push_back(T val);

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    inline void set_external_block(ssize_t row_offset, T *val, size_t size);

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    inline void set_sparse_block(ssize_t row_offset, T *ptr, size_t rows_to_write);

    inline void set_sparse_block(ChunkedBuffer&& buffer, util::BitSet&& bitset) {
        data_.buffer() = std::move(buffer);
        sparse_map_ = std::move(bitset);
    }
    
    inline void set_sparse_block(ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
        data_.buffer() = std::move(buffer);
        shapes_.buffer() = std::move(shapes);
        sparse_map_ = std::move(bitset);
    }
    ChunkedBuffer&& release_buffer();
    Buffer&& release_shapes();

    template<class T, template<class> class Tensor, std::enable_if_t<
            std::is_integral_v<T> || std::is_floating_point_v<T>,
            int> = 0>
    void set_array(ssize_t row_offset, Tensor<T> &val);

    template<class T, std::enable_if_t< std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_array(ssize_t row_offset, py::array_t<T>& val);

    void set_empty_array(ssize_t row_offset, int dimension_count);
    void set_type(TypeDescriptor td);
    ssize_t last_row() const;


    void check_magic() const;

    void unsparsify(size_t num_rows);
    void sparsify();

    void string_array_prologue(ssize_t row_offset, size_t num_strings);
    void string_array_epilogue(size_t num_strings);

    void set_string_array(ssize_t row_offset,
                          size_t string_size,
                          size_t num_strings,
                          char *input,
                          StringPool &string_pool);
    void set_string_list(ssize_t row_offset,
                         const std::vector<std::string> &input,
                         StringPool &string_pool);

    void append_sparse_map(const util::BitMagic& bv, position_t at_row);
    void append(const Column& other, position_t at_row);

    // Sorts the column by an external column's jive_table.
    // pre_allocated_space can be reused across different calls to sort_external to avoid unnecessary allocations.
    // It has to be the same size as the jive_table.
    void sort_external(const JiveTable& jive_table, std::vector<uint32_t>& pre_allocated_space);

    void mark_absent_rows(size_t num_rows);

    void default_initialize_rows(size_t start_pos, size_t num_rows, bool ensure_alloc);

    void set_row_data(size_t row_id);

    size_t get_physical_offset(size_t row) const;

    void set_sparse_map(util::BitSet&& bitset);

    std::optional<position_t> get_physical_row(position_t row) const;

    bool has_value_at(position_t row) const;

    void set_allow_sparse(Sparsity value);

    void set_shapes_buffer(size_t row_count);

    // The following two methods inflate (reduplicate) numpy string arrays that are potentially multi-dimensional,
    // i.e where the value is not a string but an array of strings
    void inflate_string_array(const TensorType<position_t>& string_refs,
                              CursoredBuffer<ChunkedBuffer>& data,
                              CursoredBuffer<Buffer> &shapes,
                              boost::container::small_vector<position_t, 1>& offsets,
                              const StringPool &string_pool);

    void inflate_string_arrays(const StringPool &string_pool);

    // Used when the column has been inflated externally, i.e. because it has be done
    // in a pipeline of tiled sub-segments
    void set_inflated(size_t inflated_count);

    bool is_inflated() const;

    void change_type(DataType target_type);

    void truncate_first_block(size_t row);

    void truncate_last_block(size_t row);

    void truncate_single_block(size_t start_offset, size_t end_offset);

    position_t row_count() const;

    std::optional<StringArrayData> string_array_at(position_t idx, const StringPool &string_pool);

    ChunkedBuffer::Iterator get_iterator() const {
        return {const_cast<ChunkedBuffer*>(&data_.buffer()), get_type_size(type_.data_type())};
    }

    size_t bytes() const {
        return data_.bytes();
    }

    ColumnData data() const {
        return ColumnData(&data_.buffer(), &shapes_.buffer(), type_, sparse_map_ ? &*sparse_map_ : nullptr);
    }

    const uint8_t* ptr() const {
        return data_.buffer().data();
    }

    uint8_t* ptr() {
        return data_.buffer().data();
    }

    TypeDescriptor type() const { return type_; }

    size_t num_blocks() const  {
        return data_.buffer().num_blocks();
    }

    const shape_t* shape_ptr() const {
        return shapes_.ptr_cast<shape_t>(0, num_shapes());
    }

    void set_orig_type(const TypeDescriptor& desc) {
        orig_type_ = desc;
    }

    bool has_orig_type() const {
        return static_cast<bool>(orig_type_);
    }

    const TypeDescriptor& orig_type() const  {
        return orig_type_.value();
    }

    void compact_blocks() {
        data_.compact_blocks();
    }

    const auto& blocks() const {
        return data_.buffer().blocks();
    }

    inline shape_t *allocate_shapes(std::size_t bytes) {
        shapes_.ensure_bytes(bytes);
        return reinterpret_cast<shape_t *>(shapes_.cursor());
    }

    inline uint8_t *allocate_data(std::size_t bytes) {
        util::check(bytes != 0, "Allocate data called with zero size");
        data_.ensure_bytes(bytes);
        return data_.cursor();
    }

    inline void advance_data(std::size_t size) {
        data_.advance(position_t(size));
    }

    inline void advance_shapes(std::size_t size) {
        shapes_.advance(position_t(size));
    }

    template<typename T>
    std::optional<T> scalar_at(position_t row) const;

    // Copies all physical scalars to a std::vector<T>. This is useful if you require many random access operations
    // and you would like to avoid the overhead of computing the exact location every time.
    template<typename T>
    std::vector<T> clone_scalars_to_vector() const;

    // N.B. returning a value not a reference here, so it will need to be pre-checked when data is sparse or it
    // will likely 'splode.
    template<typename T>
    T& reference_at(position_t row);

    template<typename T>
    std::optional<TensorType<T>> tensor_at(position_t idx) const;

    template<typename T>
    const T *ptr_cast(position_t idx, size_t required_bytes) const;

    template<typename T>
    T *ptr_cast(position_t idx, size_t required_bytes);

    [[nodiscard]] auto& buffer() {
        return data_.buffer();
    }

    uint8_t* bytes_at(size_t bytes, size_t required) {
        ARCTICDB_TRACE(log::inmem(), "Column returning {} bytes at position {}", required, bytes);
        return data_.bytes_at(bytes, required);
    }

    const uint8_t* bytes_at(size_t bytes, size_t required) const {
        return data_.bytes_at(bytes, required);
    }

    void assert_size(size_t bytes) const {
        data_.buffer().assert_size(bytes);
    }

    template<typename T>
    std::optional<position_t> search_unsorted(T val) const;

    // Only works if column is of numeric type and is monotonically increasing
    // Returns the index such that if val were inserted before that index, the order would be preserved
    // By default returns the lowest index satisfying this property. If from_right=true, returns the highest such index
    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    size_t search_sorted(T val, bool from_right=false, std::optional<int64_t> from = std::nullopt, std::optional<int64_t> to = std::nullopt) const;

    [[nodiscard]] static std::vector<std::shared_ptr<Column>> split(
        const std::shared_ptr<Column>& column,
        size_t num_rows
    );
    /// @brief Produces a new column containing only the data in range [start_row, end_row)
    /// @param[in] start_row Inclusive start of the row range
    /// @param[in] end_row Exclusive end of the row range
    [[nodiscard]] static std::shared_ptr<Column> truncate(
        const std::shared_ptr<Column>& column,
        size_t start_row,
        size_t end_row
    );

    template <
            typename input_tdt,
            typename functor>
    requires std::is_invocable_r_v<void, functor, typename input_tdt::DataTypeTag::raw_type>
    static void for_each(const Column& input_column, functor&& f);

    template <
            typename input_tdt,
            typename functor>
    requires std::is_invocable_r_v<void, functor, typename ColumnData::Enumeration<typename input_tdt::DataTypeTag::raw_type>>
    static void for_each_enumerated(const Column& input_column, functor&& f);

    template <
            typename input_tdt,
            typename output_tdt,
            typename functor>
    requires std::is_invocable_r_v<typename output_tdt::DataTypeTag::raw_type, functor, typename input_tdt::DataTypeTag::raw_type>
    static void transform(const Column& input_column, Column& output_column, functor&& f);

    template<
            typename left_input_tdt,
            typename right_input_tdt,
            typename output_tdt,
            typename functor>
    requires std::is_invocable_r_v<
            typename output_tdt::DataTypeTag::raw_type,
            functor,
            typename left_input_tdt::DataTypeTag::raw_type,
            typename right_input_tdt::DataTypeTag::raw_type>
    static void transform(const Column& left_input_column,
                          const Column& right_input_column,
                          Column& output_column,
                          functor&& f);

    template <
            typename input_tdt,
            std::predicate<typename input_tdt::DataTypeTag::raw_type> functor>
    static void transform(const Column& input_column,
                          util::BitSet& output_bitset,
                          bool sparse_missing_value_output,
                          functor&& f);

    template <
            typename left_input_tdt,
            typename right_input_tdt,
            std::relation<typename left_input_tdt::DataTypeTag::raw_type, typename right_input_tdt::DataTypeTag::raw_type> functor>
    static void transform(const Column& left_input_column,
                          const Column& right_input_column,
                          util::BitSet& output_bitset,
                          bool sparse_missing_value_output,
                          functor&& f);

    void init_buffer() {
        std::call_once(*init_buffer_, [this] () {
            extra_buffers_ = std::make_unique<ExtraBufferContainer>();
        });
    }

    ChunkedBuffer& create_extra_buffer(size_t offset, ExtraBufferType type, size_t size, AllocationType allocation_type) {
        init_buffer();
        return extra_buffers_->create_buffer(offset, type, size, allocation_type);
    }

    ChunkedBuffer& get_extra_buffer(size_t offset, ExtraBufferType type) const {
        util::check(static_cast<bool>(extra_buffers_), "Extra buffer {} requested but pointer is null", offset);
        return extra_buffers_->get_buffer(offset, type);
    }

    void set_extra_buffer(size_t offset, ExtraBufferType type, ChunkedBuffer&& buffer) {
        init_buffer();
        extra_buffers_->set_buffer(offset, type, std::move(buffer));
    }

    bool has_extra_buffer(size_t offset, ExtraBufferType type) const {
        if(!extra_buffers_)
            return false;

        return extra_buffers_->has_buffer(offset, type);
    }
private:
    position_t last_offset() const;
    void update_offsets(size_t nbytes);
    bool is_scalar() const;
    const shape_t *shape_index(position_t idx) const;
    position_t bytes_offset(position_t idx) const;
    position_t scalar_offset(position_t idx) const;
    size_t item_size() const;
    size_t inflated_row_count() const;
    size_t num_shapes() const;
    void set_sparse_bit_for_row(size_t sparse_location);
    void regenerate_offsets() const;

    // Permutes the physical column storage based on the given sorted_pos.
    void physical_sort_external(std::vector<uint32_t> &&sorted_pos);

    // Members
    CursoredBuffer<ChunkedBuffer> data_;
    CursoredBuffer<Buffer> shapes_;

    mutable boost::container::small_vector<position_t, 1> offsets_;
    TypeDescriptor type_;
    std::optional<TypeDescriptor> orig_type_;
    ssize_t last_logical_row_ = -1;
    ssize_t last_physical_row_ = -1;

    bool inflated_ = false;
    Sparsity allow_sparse_ = Sparsity::NOT_PERMITTED;

    std::optional<util::BitMagic> sparse_map_;
    FieldStatsImpl stats_;

    std::unique_ptr<std::once_flag> init_buffer_ = std::make_unique<std::once_flag>();



    std::unique_ptr<ExtraBufferContainer> extra_buffers_;
    util::MagicNum<'D', 'C', 'o', 'l'> magic_;
};

template <typename TagType>
JiveTable create_jive_table(const Column& column) {
    JiveTable output(column.row_count());
    std::iota(std::begin(output.orig_pos_), std::end(output.orig_pos_), 0);

    // Calls to scalar_at are expensive, so we precompute them to speed up the sort compare function.
    auto column_data = column.data();
    auto accessor = random_accessor<TagType>(&column_data);
    std::sort(std::begin(output.orig_pos_), std::end(output.orig_pos_),[&](const auto& a, const auto& b) -> bool {
        return accessor.at(a) < accessor.at(b);
    });

    // Obtain the sorted_pos_ by reversing the orig_pos_ permutation
    for (auto i=0u; i<output.orig_pos_.size(); ++i){
        output.sorted_pos_[output.orig_pos_[i]] = i;
    }

    return output;
}

inline JiveTable create_jive_table(const std::vector<std::shared_ptr<Column>>& columns) {
    JiveTable output(columns[0]->row_count());
    std::iota(std::begin(output.orig_pos_), std::end(output.orig_pos_), 0);

    // Calls to scalar_at are expensive, so we precompute them to speed up the sort compare function.
    for(auto it = std::rbegin(columns); it != std::rend(columns); ++it) {
        auto& column = *it;
        user_input::check<ErrorCode::E_SORT_ON_SPARSE>(!column->is_sparse(), "Can't sort on sparse column with type {}", column->type());
        details::visit_type(column->type().data_type(), [&output, &column] (auto type_desc_tag) {
            using type_info = ScalarTypeInfo<decltype(type_desc_tag)>;
            auto column_data = column->data();
            auto accessor = random_accessor<typename type_info::TDT>(&column_data);
            std::stable_sort(std::begin(output.orig_pos_),
                      std::end(output.orig_pos_),
                      [&](const auto &a, const auto &b) -> bool {
                          return accessor.at(a) < accessor.at(b);
                      });
        });
        // Obtain the sorted_pos_ by reversing the orig_pos_ permutation
        for (auto i = 0u; i < output.orig_pos_.size(); ++i) {
            output.sorted_pos_[output.orig_pos_[i]] = i;
        }
    }

    return output;
}

} //namespace arcticdb
#include "column_impl.inl"
