#pragma once

#include <arcticdb/arrow/arrow_c_data_interface.hpp>
#include <arcticdb/arrow/nanoarrow/nanoarrow_types.h>
#include <arcticdb/arrow/nanoarrow/nanoarrow.h>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

template <typename ArrowType>
struct WrapperBase {
    ArrowType *arrow_object_ = nullptr;

    WrapperBase(ArrowType* arrow_object) :
        arrow_object_(arrow_object) {
    }

    WrapperBase(WrapperBase&& other) {
        arrow_object_ = other.arrow_object_;
        other.arrow_object_ = nullptr;
    }

    WrapperBase& operator=(WrapperBase&& other) noexcept {
        if (this != &other) {
            arrow::check<ErrorCode::E_ARROW_INVALID>(arrow_object_ == nullptr, "Reassigning wrapper object");
            arrow_object_ = other.arrow_object_;
            other.arrow_object_ = nullptr;
        }
        return *this;
    }

    auto addr() const {
        return arrow_object_;
    }
};

template <typename ArrowType>
struct NonOwningWrapper : public WrapperBase<ArrowType> {
    ARCTICDB_MOVE_ONLY_DEFAULT(NonOwningWrapper)

    using BaseType = WrapperBase<ArrowType>;

    explicit NonOwningWrapper(ArrowType *arrow_object) :
        BaseType(arrow_object) {
    }
};

template<typename ArrowType>
struct OwningWrapper : public WrapperBase<ArrowType>{
    bool allocated_ = false;

    using BaseType = WrapperBase<ArrowType>;

    ARCTICDB_NO_COPY(OwningWrapper)

    OwningWrapper(OwningWrapper&& other) noexcept :
        BaseType(std::move(other)),
        allocated_(other.allocated_) {
        other.allocated_ = false;
    }

    OwningWrapper& operator=(OwningWrapper&& other) noexcept {
        if (this != &other) {
            allocated_ = other.allocated_;
            other.allocated_ = false;
        }
        return *this;
    }

    OwningWrapper() :
        BaseType(new ArrowType()),
        allocated_(true) {
    }

    explicit OwningWrapper(ArrowType *arrow_object) :
        BaseType(arrow_object) {
    }

    ~OwningWrapper() {
        auto arrow_object = BaseType::arrow_object_;
        if (arrow_object != nullptr && arrow_object->release != nullptr)
            arrow_object->release(arrow_object);

        if (allocated_)
            delete arrow_object;
    }
};


using SchemaWrapper = OwningWrapper<ArrowSchema>;

class ArrayViewWrapper {
private:
    ArrowArrayView *array_view_ = nullptr;
    bool allocated_ = false;

public:
    ARCTICDB_NO_COPY(ArrayViewWrapper)

    ArrayViewWrapper() :
        array_view_(new ArrowArrayView()),
        allocated_(true) {
    }

    ArrayViewWrapper(ArrayViewWrapper&& other) {
        if(this != &other) {
            array_view_ = other.array_view_;
            other.array_view_ = nullptr;
            other.allocated_ = false;

        }
    }

    ArrayViewWrapper& operator=(ArrayViewWrapper&& other) {
        if(this != &other) {
            arrow::check<ErrorCode::E_ARROW_INVALID>(array_view_ == nullptr, "Reassigning array view wrapper");
            array_view_ = other.array_view_;
            other.array_view_ = nullptr;
            other.allocated_ = false;

        }
        return *this;
    }

    explicit ArrayViewWrapper(ArrowArrayView *array_view) :
        array_view_(array_view) {
    }

    void init() {
        ArrowArrayViewInitFromType(array_view_, NANOARROW_TYPE_UNINITIALIZED);
    }

    ArrowArrayView* array_view() {
        return array_view_;
    }

    ~ArrayViewWrapper() {
        if(array_view_ != nullptr) {
            ArrowArrayViewReset(array_view_);
            if (allocated_)
                delete array_view_;
        }
        allocated_ = false;
    }

    ArrowArrayView* addr() {
        return array_view_;
    }
};

} // namespace arcticdb