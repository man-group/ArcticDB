#pragma once

#include <variant>

#include <azure/identity/default_azure_credential.hpp>

namespace arcticdb::storage {

    using VariantStorageCredential = std::variant<std::monostate, std::shared_ptr<Azure::Core::Credentials::TokenCredential>>;

    class StorageCredential {
    private:
        VariantStorageCredential storage_credential_;

    public:
        const VariantStorageCredential& variant() const {
            return storage_credential_;
        }

        void set_azure_credential(std::shared_ptr<Azure::Core::Credentials::TokenCredential>& storage_credential) {
            storage_credential_ = storage_credential;
        }
    };
}
