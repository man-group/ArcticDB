#pragma once

#include <codec/encoded_field_collection.hpp>

namespace arcticdb {

using VariantField = std::variant<const EncodedField *, const arcticdb::proto::encoding::EncodedField *>;

struct VariantEncodedFieldCollection {
    EncodedFieldCollection fields_;
    const arcticdb::proto::encoding::SegmentHeader *header_ = nullptr;
    bool is_proto_ = false;

    explicit VariantEncodedFieldCollection(const Segment &segment);

    [[nodiscard]] VariantField at(size_t pos) const {
        if (is_proto_)
            return &header_->fields(static_cast<int>(pos));
        else
            return &fields_.at(pos);
    }

    [[nodiscard]] size_t size() const {
        if (is_proto_)
            return header_->fields_size();
        else
            return fields_.size();
    }
};

} // namespace arcticdb