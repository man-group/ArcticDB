#include <arcticdb/arrow/arrow_array.hpp>

namespace arcticdb {

ArrayChildren::ArrayChildren(std::shared_ptr<const Array> parent) :
        parent_(std::move(parent)),
        length_(parent_->ptr()->n_children) {
}

std::shared_ptr<Array> ArrayChildren::operator[](size_t k) const {
    auto index = static_cast<int>(k);
    if (index < 0 || index >= length_) {
        throw std::out_of_range("Index out of range");
    }
    return std::make_shared<Array>(parent_, Array::NonOwnedArray{child_addr(index)}, std::make_shared<Schema>(parent_->schema().children()[index]));  //TODo non-owning?
}

ArrowArray* ArrayChildren::child_addr(int64_t i) const {
    ArrowArray** children = parent_->ptr()->children;
    return children[i];
}

ArrayViewChildren::ArrayViewChildren(std::shared_ptr<const ArrayView> parent) :
        parent_(std::move(parent)),
        length_(parent_->ptr()->n_children) {
}

ArrowArrayView* ArrayViewChildren::child_addr(int64_t i) const {
    ArrowArrayView** children = parent_->ptr()->children;
    return children[i];
}

ArrayView ArrayViewChildren::operator[](size_t k) {
    auto index = static_cast<int>(k);
    if (index < 0 || index >= length_) {
        throw std::out_of_range("Index out of range");
    }
    return {parent_, child_addr(index), std::make_shared<Schema>(parent_->schema()->children()[index])};
}

ArrayViewBuffers::ArrayViewBuffers(std::shared_ptr<ArrayView> array_view) :
        array_view_(std::move(array_view)) {
    array_view_ = array_view;
    length_ = 3;
    for (int i = 0; i < 3; i++) {
        if (array_view_->ptr()->layout.buffer_type[i] == NANOARROW_BUFFER_TYPE_NONE) {
            length_ = i;
            break;
        }
    }
}

std::optional<arrow::BufferView> ArrayViewBuffers::operator[](int64_t k) {
    k = static_cast<int>(k);
    if (k < 0 || k >= length_) {
        throw std::out_of_range("Index out of range");
    }
    ArrowBufferView* buffer_view = &array_view_->ptr()->buffer_views[k];
    if (buffer_view->data.data == nullptr)
        return std::nullopt;

    return arrow::BufferView(
        array_view_,
        reinterpret_cast<uintptr_t>(buffer_view),
        array_view_->ptr()->layout.buffer_data_type[k],
        array_view_->ptr()->layout.element_size_bits[k]
    );
}

ArrayView Array::view() const {
    arrow::check<ErrorCode::E_ARROW_INVALID>(arrow_type_ != NANOARROW_TYPE_UNINITIALIZED, "Uninitialized arrow type in view()");
    ArrayViewWrapper wrapper;
    ArrowArrayViewInitFromType(wrapper.array_view(), arrow_type_);

    Error error;
    auto result = ArrowArrayViewSetArray(wrapper.array_view(), ptr_, &error.c_error);
    if (result != NANOARROW_OK) {
        util::raise_rte("ArrowArrayViewSetArray()", result);
    }

    return {shared_from_this(), std::move(wrapper), schema_};
}

void Array::ensure(int64_t length, size_t bytes) {
    ARCTICDB_ARROW_CHECK(ArrowArrayStartAppending(ptr_))
    ARCTICDB_ARROW_CHECK(ArrowArrayReserve(ptr_, bytes))
    ArrowError error{};
    ARCTICDB_ARROW_CHECK(ArrowArrayFinishBuildingDefault(ptr_, &error))
    ArrowArrayViewSetLength(view().ptr(), length);
    ptr_->length = length;
}


} // namespace arcticdb