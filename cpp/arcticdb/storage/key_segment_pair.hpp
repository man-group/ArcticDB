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

            KeySegmentData() = default;
            explicit KeySegmentData(VariantKey &&key) : key_(std::move(key)), segment_() {}
            KeySegmentData(VariantKey &&key, Segment &&segment) : key_(std::move(key)), segment_(std::move(segment)) {}

            ARCTICDB_NO_MOVE_OR_COPY(KeySegmentData)
        };

    public:
        KeySegmentPair() = default;
        explicit KeySegmentPair(VariantKey &&key)
          : key_(std::make_shared<VariantKey>(std::move(key))) {}

        KeySegmentPair(VariantKey &&key, Segment &&segment)
          : key_(std::make_shared<VariantKey>(std::move(key))),
            segment_(std::make_shared<Segment>(std::move(segment))) {}

        template<typename K>
        KeySegmentPair(K &&key, std::shared_ptr<Segment> segment)
            : key_(std::make_shared<VariantKey>(std::forward<K>(key))),
              segment_(std::move(segment)) {}

        ARCTICDB_MOVE_COPY_DEFAULT(KeySegmentPair)

        // TODO remove
        Segment &segment() {
            return *segment_;
        }

        Segment&& release_segment() {
            auto tmp = std::make_shared<Segment>();
            std::swap(tmp, segment_);
            return std::move(*tmp);
        }

        [[nodiscard]] std::shared_ptr<Segment> segment_ptr() const {
          return segment_;
        }

        template<typename T>
        void set_key(T&& key) {
          key_ = std::make_shared<VariantKey>(std::forward<T>(key));
        }

        [[nodiscard]] const AtomKey &atom_key() const {
            util::check(std::holds_alternative<AtomKey>(variant_key()), "Expected atom key access");
            return std::get<AtomKey >(variant_key());
        }

        [[nodiscard]] const RefKey &ref_key() const {
            util::check(std::holds_alternative<RefKey>(variant_key()), "Expected ref key access");
            return std::get<RefKey >(variant_key());
        }

        [[nodiscard]] const VariantKey& variant_key() const {
            return *key_;
        }

        [[nodiscard]] const Segment &segment() const {
            return *segment_;
        }

        [[nodiscard]] bool has_segment() const {
            return !segment().is_empty();
        }

        [[nodiscard]] std::string_view key_view() const {
            return variant_key_view(variant_key());
        }

        [[nodiscard]] KeyType key_type() const {
            return variant_key_type(variant_key());
        }

    private:
        std::shared_ptr<VariantKey> key_ = std::make_shared<VariantKey>();
        std::shared_ptr<Segment> segment_ = std::make_shared<Segment>();
    };

} //namespace arcticdb