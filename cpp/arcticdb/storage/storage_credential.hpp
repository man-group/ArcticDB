#pragma once

#include <azure/identity/default_azure_credential.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb::storage {

    using AzureBaseCredential = Azure::Core::Credentials::TokenCredential;
    using VariantStorageCredential = std::variant<std::monostate, std::shared_ptr<AzureBaseCredential>>;
    class StorageCredential {
    private:
        VariantStorageCredential storage_credential_;

    public:
        const VariantStorageCredential& variant() const {
            return storage_credential_;
        }

        void set_azure_credential(std::shared_ptr<AzureBaseCredential>& storage_credential) {
            storage_credential_ = storage_credential;
        }

        [[nodiscard]] std::shared_ptr<AzureBaseCredential> get_azure_credential() const{
            return util::variant_match(storage_credential_, 
                [](const std::shared_ptr<AzureBaseCredential>& storage_credential) {
                    return storage_credential;
                },
                [](std::monostate) {
                    return std::shared_ptr<AzureBaseCredential>();
                }
            );
        }
    };
}
