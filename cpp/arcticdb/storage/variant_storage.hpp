/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/util/type_traits.hpp>
#include <arcticdb/storage/failure_simulation.hpp>
#include <arcticdb/util/composite.hpp>

namespace arcticdb::storage::variant {

template<class VariantStorageType>
class VariantStorage final : public Storage<VariantStorage<VariantStorageType>> {

    static_assert(util::is_instantiation_of_v<VariantStorageType, std::variant>);

    using Parent = Storage<VariantStorage>;
    friend Parent;

  public:

    template<class T, std::enable_if_t<!std::is_same_v<std::decay_t<T>, VariantStorage>, int> = 0>
    explicit VariantStorage(T &&v) :
        Parent(v.library_path(), v.open_mode()), variant_(std::move(v)){}

    template<class Lambda>
    auto delegate(Lambda &&l) {
        return std::visit([&](auto &&impl) {
            using Impl = std::decay_t<decltype(impl)>;
            if constexpr (std::is_same_v<Impl, std::monostate>) {
                throw std::runtime_error("Cannot call method on monostate");
            } else {
                return l(impl);
            }
        }, variant_);
    }

    /**
     * Previously, to add any public method to any storage requires changing two dozen files (and more or less recompile
     * the entire code base over and over while prototyping). This visitor style method makes it much more manage-able
     * to surface features specific to one type of Storage.
     *
     * Remember to check the open mode.
     *
     * The visitor cannot return anything to simplify templating.
     */
    template<class Visitor>
    void storage_specific(Visitor&& visitor) {
        util::variant_match(variant_,
                std::forward<Visitor>(visitor),
                [](const StorageBase&) { /* Do nothing if the visitor's type didn't match. */});
    }

  protected:
    void do_write(Composite<KeySegmentPair>&& kvs) {
        return delegate([&](auto &&impl) {
            return impl.write(std::move(kvs));
        });
    }

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
        return delegate([&](auto &&impl) {
            return impl.update(std::move(kvs), opts);
        });
    }

    template<class Visitor>
    void do_read(Composite<VariantKey>&& ks, Visitor &&visitor, ReadKeyOpts opts) {
        return delegate([&](auto &&impl) {
            return impl.read(std::move(ks), std::move(visitor), opts);
        });
    }

    void do_remove(Composite<VariantKey> ks, RemoveOpts opts) {
         delegate([&](auto &&impl) {
             impl.remove(std::move(ks), opts);
        });
    }

    void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) {
         delegate([&](auto &&impl) {
             impl.iterate_type(key_type, visitor, prefix);
        });
    }

    bool do_supports_prefix_matching() {
        return delegate([&](auto &&impl) {
            return impl.supports_prefix_matching();
        });
    }

    bool do_fast_delete() {
        return delegate([&](auto &&impl) {
            return impl.fast_delete();
        });
    }

    bool do_key_exists(const VariantKey& key ) {
        return delegate([&](auto &&impl) {
            return impl.key_exists(key);
        });
    }

  private:
    VariantStorageType variant_;
};

} //namespace arcticdb::storage