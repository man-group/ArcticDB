/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/codec/segment.hpp>

namespace arcticdb::storage {
    using namespace entity;

    /*
     * KeySegmentPair contains compressed data as returned from storage. Unlike FrameSlice, this does
     * not contain any positioning information for the contained data.
     */
    class KeySegmentPair {

        struct KeySegmentData {
            VariantKey key_;
            Segment segment_;
            size_t id_;

            KeySegmentData() = default;
            explicit KeySegmentData(VariantKey &&key) : key_(std::move(key)), segment_(), id_(0) {}
            KeySegmentData(VariantKey &&key, Segment &&segment, size_t id) : key_(std::move(key)), segment_(std::move(segment)), id_(id) {}

            ARCTICDB_NO_MOVE_OR_COPY(KeySegmentData)
        };

    public:
        KeySegmentPair() : data_(std::make_shared<KeySegmentData>()) {}
        explicit KeySegmentPair(VariantKey &&key) : data_(std::make_shared<KeySegmentData>(std::move(key))) {}
        KeySegmentPair(VariantKey &&key, Segment&& segment) :
            data_(std::make_shared<KeySegmentData>(std::move(key), std::move(segment), 0)) {}
        KeySegmentPair(VariantKey &&key, Segment&& segment, size_t id) :
            data_(std::make_shared<KeySegmentData>(std::move(key), std::move(segment), id)) {}

        ARCTICDB_MOVE_COPY_DEFAULT(KeySegmentPair)

        Segment &segment() {
            return data_->segment_;
        }

        Segment&& release_segment() {
            return std::move(data_->segment_);
        }

        AtomKey &atom_key() {
            util::check(std::holds_alternative<AtomKey>(variant_key()), "Expected atom key access");
            return std::get<AtomKey >(variant_key());
        }

        const AtomKey &atom_key() const {
            util::check(std::holds_alternative<AtomKey>(variant_key()), "Expected atom key access");
            return std::get<AtomKey >(variant_key());
        }

        RefKey &ref_key() {
            util::check(std::holds_alternative<RefKey>(variant_key()), "Expected ref key access");
            return std::get<RefKey >(variant_key());
        }

        const RefKey &ref_key() const {
            util::check(std::holds_alternative<RefKey>(variant_key()), "Expected ref key access");
            return std::get<RefKey >(variant_key());
        }

        VariantKey& variant_key() {
            return data_->key_;
        }

        const VariantKey& variant_key() const {
            return data_->key_;
        }

        const Segment &segment() const {
            return data_->segment_;
        }

        bool has_segment() const {
            return !segment().is_empty();
        }

        std::string_view key_view() const {
            return variant_key_view(variant_key());
        }

        KeyType key_type() const {
            return variant_key_type(variant_key());
        }

        size_t id() {
            return data_->id_;
        }
    private:
        std::shared_ptr<KeySegmentData> data_;
    };

    template<class T, std::enable_if_t<std::is_same_v<T, EnvironmentName> || std::is_same_v<T, StorageName>, int> = 0>
    bool operator==(const T &l, const T &r) {
        return l.value == r.value;
    }



} //namespace arcticdb