#include <arcticdb/arrow/arrow_schema.hpp>

namespace arcticdb {

SchemaChildren::SchemaChildren(std::shared_ptr<const Schema> parent) :
    parent_(std::move(parent)),
    length_(parent_->ptr()->n_children){
}

Schema SchemaChildren::operator[](int k) const {
    arrow::check<ErrorCode::E_ARROW_INVALID>(k >= 0 || k < length_,"Index {} out of range in SchemaChildren", k);
    return Schema{parent_, NonOwningWrapper<ArrowSchema>{child_addr(k)}};
}

ArrowSchema* SchemaChildren::child_addr(int64_t i) const {
    ArrowSchema** children = parent_->ptr()->children;
    ArrowSchema* child = children[i];
    return child;
}

}  //namespace arcticdb