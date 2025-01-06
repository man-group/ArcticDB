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

    public:
        KeySegmentPair() = default;
        explicit KeySegmentPair(VariantKey &&key)
          : key_(std::make_shared<VariantKey>(std::move(key))) {}

        KeySegmentPair(VariantKey&& key, std::unique_ptr<Segment> segment)
            : key_(std::make_shared<VariantKey>(std::move(key))),
            segment_(std::move(segment)) {

        }

        KeySegmentPair(VariantKey &&key, Segment &&segment)
          : KeySegmentPair(std::move(key), std::make_unique<Segment>(std::move(segment))) {

        }

        ARCTICDB_MOVE_ONLY_DEFAULT(KeySegmentPair)

        std::unique_ptr<Segment> release_segment() {
            util::check(static_cast<bool>(segment_), "Attempting to access segment_ but it has not been set");
            return std::move(segment_);
        }

        [[nodiscard]] Segment& segment() const {
          util::check(static_cast<bool>(segment_), "Attempting to access segment_ptr but it has not been set");
          return *segment_;
        }

        template<typename T>
        void set_key(T&& key) {
          key_ = std::make_shared<VariantKey>(std::forward<T>(key));
        }

        void set_segment(Segment&& segment) {
          segment_ = std::make_unique<Segment>(std::move(segment));
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
            util::check(key_, "Attempting to access key_ but it has not been set");
            return *key_;
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
        std::unique_ptr<Segment> segment_ = std::make_unique<Segment>();
    };

} //namespace arcticdb