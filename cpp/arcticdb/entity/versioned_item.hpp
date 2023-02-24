/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <fmt/format.h>
#include <string>


namespace arcticdb {
struct VersionedItem {
    VersionedItem(entity::AtomKey &&key) :
        key_(std::move(key)) {
    }

    VersionedItem(const entity::AtomKey& key) :
        key_(key) {
    }

    VersionedItem() = default;

    entity::AtomKey key_;

    std::string symbol() const { return fmt::format("{}", key_.id()); }
    uint64_t version() const { return key_.version_id(); }
};
}