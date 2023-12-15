#pragma once

#include <arcticdb/arrow/arrow_common.hpp>
#include <arcticdb/arrow/arrow_c_data_interface.hpp>
#include <arcticdb/arrow/arrow_stream_interace.hpp>
#include <arcticdb/arrow/arrow_wrappers.h>
#include <arcticdb/arrow/arrow_buffer.hpp>
#include <arcticdb/arrow/arrow_schema.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <vector>

namespace arcticdb {

class Array;

class ArrayChildren {
private:
    std::shared_ptr<const Array> parent_;
    int64_t length_;

public:
    explicit ArrayChildren(std::shared_ptr<const Array> parent);

    [[nodiscard]] size_t length() const {
        return length_;
    }

    std::shared_ptr<Array> operator[](size_t k) const;
private:
    [[nodiscard]] ArrowArray* child_addr(int64_t i) const;
};

class ArrayView;

class ArrayViewChildren {
private:
    std::shared_ptr<const ArrayView> parent_;
    int64_t length_;

public:
    explicit ArrayViewChildren(std::shared_ptr<const ArrayView> parent);

    [[nodiscard]] size_t length() const {
        return length_;
    }

    ArrayView operator[](size_t k);

private:
    [[nodiscard]] ArrowArrayView* child_addr(int64_t i) const;
};

class Array : public std::enable_shared_from_this<Array> {
public:
    using OwnedArray = OwningWrapper<ArrowArray>;
    using NonOwnedArray = NonOwningWrapper<ArrowArray>;
    using VariantWrapper = std::variant<OwnedArray, NonOwnedArray>;

    ARCTICDB_MOVE_ONLY_DEFAULT(Array)

    ArrowArray* wrapper_addr() {
        return util::variant_match(wrapper_, [] (const auto& wrapper) { return wrapper.addr(); });
    }

    Array() :
        wrapper_(OwnedArray{}),
        ptr_(wrapper_addr()) {
    }

    explicit Array(const std::shared_ptr<const Schema>& schema) :
        wrapper_(OwnedArray{}),
        ptr_(wrapper_addr()),
        schema_(schema) {
        set_type_from_schema();
    }

    Array(std::shared_ptr<const Array> parent, VariantWrapper &&wrapper) :
        parent_(std::move(parent)),
        wrapper_(std::move(wrapper)),
        ptr_(reinterpret_cast<ArrowArray*>(wrapper_addr())) {
    }

    Array(std::shared_ptr<const Array> parent, VariantWrapper &&wrapper, std::shared_ptr<Schema> schema) :
        parent_(std::move(parent)),
        wrapper_(std::move(wrapper)),
        ptr_(reinterpret_cast<ArrowArray *>(wrapper_addr())),
        schema_(std::move(schema)) {
        set_type_from_schema();
    }

    void initialize() {
        arrow::check<ErrorCode::E_ARROW_INVALID>(static_cast<bool>(schema_), "Schema not set in array initialize");
        set_type_from_schema();
        ARCTICDB_ARROW_CHECK(ArrowArrayInitFromType(ptr_, arrow_type_))
    }

    void set_schema(std::shared_ptr<Schema>&& schema) {
        schema_ = std::move(schema);
        set_type_from_schema();
    }

    void set_type_from_schema() {
        arrow_type_ = schema_->view().type();
    }

    void ensure(int64_t length, size_t bytes);

    [[nodiscard]] ArrowArray* ptr() const {
        return ptr_;
    }

    [[nodiscard]] uintptr_t addr() const {
        return reinterpret_cast<uintptr_t>(ptr_);
    }

    [[nodiscard]] bool is_valid() const  {
        return ptr_ != nullptr && ptr_->release != nullptr;
    }

    void assert_valid() const {
        if (ptr_ == nullptr)
            throw std::runtime_error("Array is NULL");
        if (ptr_->release == nullptr)
            throw std::runtime_error("Array is released");
    }

    const Schema &schema() const {
        return *schema_;
    }

    [[nodiscard]] size_t length() const {
        assert_valid();
        return ptr_->length;
    }

    [[nodiscard]] size_t offset() const {
        assert_valid();
        return ptr_->offset;
    }

    [[nodiscard]] int null_count() const {
        return static_cast<int>(ptr_->null_count);
    }

    int64_t num_buffers() const {
        return ptr_->n_buffers;
    }

    std::vector<uintptr_t> buffers() {
        std::vector<uintptr_t> buffer_addrs;
        for (int i = 0; i < ptr_->n_buffers; i++) {
            buffer_addrs.push_back(reinterpret_cast<uintptr_t>(ptr_->buffers[i]));
        }
        return buffer_addrs;
    }

    int64_t num_children() const {
        assert_valid();
        return ptr_->n_children;
    }

    ArrayChildren children() const {
        return ArrayChildren(shared_from_this());
    }

    std::optional<Array> dictionary() {
        assert_valid();
        if (ptr_->dictionary != nullptr) {
            auto schema_dictionary = schema_->dictionary();
            arrow::check<ErrorCode::E_ARROW_INVALID>(schema_dictionary.has_value(), "Null schema dictionary");
            return Array(shared_from_this(), NonOwnedArray{ptr_->dictionary}, std::make_shared<Schema>(std::move(*schema_dictionary)));
        }

        return std::nullopt;
    }

    ArrayView view() const;

    ArrowBuffer* get_bitmap_buffer() const {
        return get_buffer_impl(0);
    }

    ArrowBuffer* get_data_buffer(size_t buffer_num) const {
        return get_buffer_impl(buffer_num);
    }
private:
    ArrowBuffer* get_buffer_impl(size_t i) const {
        return ArrowArrayBuffer(ptr_, static_cast<int64_t>(i));
    }

    std::shared_ptr<const Array> parent_;
    VariantWrapper wrapper_;
    ArrowArray *ptr_ = nullptr;
    ArrowType arrow_type_ = NANOARROW_TYPE_UNINITIALIZED;
    std::shared_ptr<const Schema> schema_;
};

class ArrayViewBuffers {
private:
    std::shared_ptr<ArrayView> array_view_;
    int64_t length_;

public:
    explicit ArrayViewBuffers(std::shared_ptr<ArrayView> array_view);

    [[nodiscard]] int64_t length() const {
        return length_;
    }

    std::optional<arrow::BufferView> operator[](int64_t k);
};

class ArrayView : public std::enable_shared_from_this<ArrayView> {

private:
    std::shared_ptr<const ArrayView> view_parent_;
    std::shared_ptr<const Array> parent_;
    ArrayViewWrapper wrapper_;
    ArrowArrayView *ptr_;
    std::shared_ptr<const Schema> schema_;

public:
    ARCTICDB_MOVE_ONLY_DEFAULT(ArrayView)

    ArrayView(ArrowArrayView* addr, std::shared_ptr<Schema> schema) :
        ptr_(addr),
        schema_(std::move(schema)) {
    }

    ArrayView(std::shared_ptr<const ArrayView> parent, ArrowArrayView* addr, std::shared_ptr<const Schema> schema) :
        view_parent_(std::move(parent)),
        ptr_(addr),
        schema_(std::move(schema)) {
    }

    ArrayView(std::shared_ptr<const Array> parent, ArrayViewWrapper&& wrapper, std::shared_ptr<const Schema> schema) :
        parent_(std::move(parent)),
        wrapper_(std::move(wrapper)),
        ptr_(wrapper_.addr()),
        schema_(std::move(schema)) {
    }

   /*ArrayView(std::shared_ptr<const Array> parent, ArrowArrayView* addr, std::shared_ptr<const Schema> schema) :
        parent_(std::move(parent)),
        ptr_(addr),
        schema_(std::move(schema)) {
    } */

    [[nodiscard]] size_t length() const {
        return ptr_->length;
    }

    [[nodiscard]] size_t offset() const {
        return ptr_->offset;
    }

    [[nodiscard]] int null_count() const {
        return static_cast<int>(ptr_->null_count);
    }

    ArrowArrayView* ptr() const {
        return ptr_;
    }

    ArrayViewChildren children() {
        return ArrayViewChildren(shared_from_this());
    }

    ArrayViewBuffers buffers() {
        return ArrayViewBuffers{shared_from_this()};
    }
    
    const ArrowLayout& layout() {
        return ptr_->layout;
    }

    std::optional<ArrayView> dictionary() {
        if (ptr_->dictionary != nullptr) {
            auto schema_dictionary = schema_->dictionary();
            arrow::check<ErrorCode::E_ARROW_INVALID>(schema_dictionary.has_value(), "Null schema dictionary");
            return ArrayView(shared_from_this(), ptr_->dictionary, std::make_shared<Schema>(std::move(*schema_dictionary)));
        }

        return std::nullopt;
    }

    [[nodiscard]] std::shared_ptr<const Schema> schema() const {
        return schema_;
    }
};

}
