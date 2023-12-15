#pragma once

#include <arcticdb/arrow/arrow_c_data_interface.hpp>
#include <arcticdb/arrow/arrow_wrappers.h>
#include <arcticdb/arrow/nanoarrow/nanoarrow.h>
#include <arcticdb/arrow/arrow_error.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

class SchemaChildren;
class SchemaMetadata;
class SchemaChildren;

class SchemaView {
private:
    ArrowSchemaView schema_view_ = {};

public:
    SchemaView() {
        schema_view_.type = NANOARROW_TYPE_UNINITIALIZED;
        schema_view_.storage_type = NANOARROW_TYPE_UNINITIALIZED;
    }

    ArrowSchemaView &schema_view() {
        return schema_view_;
    }

    [[nodiscard]] std::string type_string() const {
        const char *type_str = ArrowTypeString(schema_view_.type);
        if (type_str != nullptr) {
            return {type_str};
        }
        return "";
    }

    [[nodiscard]] ArrowType type() const {
        return schema_view_.type;
    }

    [[nodiscard]] std::string storage_type() const {
        const char *type_str = ArrowTypeString(schema_view_.storage_type);
        if (type_str != nullptr) {
            return {type_str};
        }
        return {};
    }

    [[nodiscard]] int fixed_size() const {
        if (schema_view_.type == NANOARROW_TYPE_FIXED_SIZE_LIST || schema_view_.type == NANOARROW_TYPE_FIXED_SIZE_BINARY)
            return schema_view_.fixed_size;

        return 0;
    }

    [[nodiscard]] int decimal_bitwidth() const {
        if (schema_view_.type == NANOARROW_TYPE_DECIMAL128 || schema_view_.type == NANOARROW_TYPE_DECIMAL256)
            return schema_view_.decimal_bitwidth;

        return 0;
    }

    [[nodiscard]] int decimal_precision() const {
        if (schema_view_.type == NANOARROW_TYPE_DECIMAL128 || schema_view_.type == NANOARROW_TYPE_DECIMAL256)
            return schema_view_.decimal_precision;

        return 0;
    }

    [[nodiscard]] int decimal_scale() const {
        if (schema_view_.type == NANOARROW_TYPE_DECIMAL128 || schema_view_.type == NANOARROW_TYPE_DECIMAL256)
            return schema_view_.decimal_scale;

        return 0;
    }

    [[nodiscard]] std::string time_unit() const {
        if (schema_view_.type == NANOARROW_TYPE_TIME32
            || schema_view_.type == NANOARROW_TYPE_TIME64
            || schema_view_.type == NANOARROW_TYPE_DURATION
            || schema_view_.type == NANOARROW_TYPE_TIMESTAMP) {
            const char *time_unit_str = ArrowTimeUnitString(schema_view_.time_unit);
            if (time_unit_str != nullptr) {
                return {time_unit_str};
            }
        }
        return "";
    }

    [[nodiscard]] std::string timezone() const {
        if (schema_view_.type == NANOARROW_TYPE_TIMESTAMP && schema_view_.timezone != nullptr) {
            return {schema_view_.timezone};
        }
        return "";
    }

    [[nodiscard]] std::vector<int> union_type_ids() const {
        std::vector<int> type_ids;
        if (schema_view_.type == NANOARROW_TYPE_DENSE_UNION || schema_view_.type == NANOARROW_TYPE_SPARSE_UNION) {
            std::string type_ids_str(schema_view_.union_type_ids);
            size_t pos;
            while ((pos = type_ids_str.find(',')) != std::string::npos) {
                type_ids.push_back(std::stoi(type_ids_str.substr(0, pos)));
                type_ids_str.erase(0, pos + 1);
            }
            type_ids.push_back(std::stoi(type_ids_str));
        }
        return type_ids;
    }

    [[nodiscard]] std::string extension_name() const {
        if (schema_view_.extension_name.data != nullptr) {
            return std::string(schema_view_.extension_name.data, schema_view_.extension_name.size_bytes);
        }
        return "";
    }

    [[nodiscard]] std::string extensionmetadata_() const {
        if (schema_view_.extension_name.data != nullptr) {
            return std::string(schema_view_.extension_metadata.data, schema_view_.extension_metadata.size_bytes);
        }
        return "";
    }
};

class Schema;

class SchemaChildren {
private:
    std::shared_ptr<const Schema> parent_;
    int64_t length_;

public:
    explicit SchemaChildren(std::shared_ptr<const Schema> parent);

    [[nodiscard]] int __len__() const {
        return length_;
    }

    Schema operator[](int k) const;

    ArrowSchema* child_addr(int64_t i) const;
};

class SchemaMetadata {
private:
    std::shared_ptr<Schema> parent_;
    std::uintptr_t metadata_;
    std::vector<std::pair<std::string, std::string>> metadata_pairs_;

public:
    SchemaMetadata(std::shared_ptr<Schema> parent, std::uintptr_t metadata) :
        parent_(std::move(parent)),
        metadata_(metadata) {}

    void initReader() {
        // Implement ArrowMetadataReaderInit function here
        // You'll need to handle the error case as well
    }

    std::uintptr_t ptr() const {
        return metadata_;
    }

    size_t size() {
        initReader();
        return metadata_pairs_.size();
    }

    std::vector<std::pair<std::string, std::string>>::iterator begin() {
        initReader();
        return metadata_pairs_.begin();
    }

    std::vector<std::pair<std::string, std::string>>::iterator end() {
        initReader();
        return metadata_pairs_.end();
    }
};

class Schema : public std::enable_shared_from_this<Schema> {
private:
    std::shared_ptr<const Schema> parent_;
    WrapperBase<ArrowSchema> base_;
    ArrowSchema *ptr_ = nullptr;

    void assert_valid() const {
        arrow::check<ErrorCode::E_ARROW_INVALID>(ptr_ != nullptr, "Schema is NULL");
        arrow::check<ErrorCode::E_ARROW_INVALID>(ptr_->release != nullptr, "Schema is released");
    }

public:
    Schema(std::shared_ptr<const Schema> parent, WrapperBase<ArrowSchema> &&base) :
        parent_(std::move(parent)),
        base_(std::move(base)),
        ptr_(reinterpret_cast<ArrowSchema *>(base_.addr())) {
        assert_valid();
    }

    Schema() :
            base_(SchemaWrapper{}),
            ptr_(reinterpret_cast<ArrowSchema *>(base_.addr())) {
    }

    Schema(ArrowType arrow_type) :
            base_(SchemaWrapper{}),
            ptr_(reinterpret_cast<ArrowSchema *>(base_.addr())) {
        ArrowSchemaInitFromType(ptr_, arrow_type);
        assert_valid();
    }

    [[nodiscard]] ArrowSchema *ptr() const {
        return ptr_;
    }

    [[nodiscard]] uintptr_t addr() const {
        return reinterpret_cast<uintptr_t>(ptr_);
    }

    [[nodiscard]] bool is_valid() const {
        return ptr_ != nullptr && ptr_->release != nullptr;
    }

    [[nodiscard]] std::string __repr__() const {
        int64_t n_chars = ArrowSchemaToString(ptr_, nullptr, 0, true);
        std::string out_str(n_chars + 1, '\0');
        ArrowSchemaToString(ptr_, reinterpret_cast<char *>(out_str.data()), n_chars + 1, true);
        return out_str;
    }

    [[nodiscard]] std::string format() const {
        assert_valid();
        if (ptr_->format != nullptr) {
            return {ptr_->format};
        }
        return {};
    }

    [[nodiscard]] std::string name() const {
        assert_valid();
        if (ptr_->name != nullptr) {
            return {ptr_->name};
        }
        return "";
    }

    [[nodiscard]] int flags() const {
        return static_cast<int>(ptr_->flags);
    }

    std::optional<SchemaMetadata> metadata() {
        assert_valid();
        if (ptr_->metadata != nullptr) {
            return SchemaMetadata(shared_from_this(), reinterpret_cast<uintptr_t>(ptr_->metadata));
        }
        return std::nullopt;
    }

    int64_t num_children() const {
        return ptr_->n_children;
    }

    SchemaChildren children() const {
        assert_valid();
        return SchemaChildren{shared_from_this()};
    }

    std::optional<Schema> dictionary() const {
        assert_valid();
        if (ptr_->dictionary != nullptr) {
            return Schema{shared_from_this(), SchemaWrapper{ptr_->dictionary}};
        }
        return std::nullopt;
    }

    SchemaView view() const {
        assert_valid();
        SchemaView schema_view;
        Error error;
        int result = ArrowSchemaViewInit(&schema_view.schema_view(), ptr_, &error.c_error);
        if (result != NANOARROW_OK) {
            error.raise_message("ArrowSchemaViewInit()", result);
        }

        return schema_view;
    }
};

} // namespace arcticdb