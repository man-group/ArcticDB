/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

namespace arcticdb {

using namespace arcticdb::entity;

class DeDupMap {
public:
  DeDupMap()
      : de_dup_map_(
            std::make_unique<std::unordered_map<ContentHash, std::vector<AtomKey>>>()) {
  }

  std::optional<AtomKey> get_key_if_present(const AtomKey& key) const {
    if (!de_dup_map_) {
      util::raise_rte("Invalid de dup map object");
    }
    auto de_dup_candidates = de_dup_map_->find(key.content_hash());

    if (de_dup_candidates == de_dup_map_->end()) {
      return std::nullopt;
    }
    // Just content hash matching isn't enough, start and end index also need to be
    // matched which uniquely identifies the position of the segment
    auto key_iterator =
        std::find_if(std::begin(de_dup_candidates->second),
                     std::end(de_dup_candidates->second), [&](const auto& k) {
                       return k.start_index() == key.start_index() &&
                              k.end_index() == key.end_index();
                     });
    if (key_iterator == de_dup_candidates->second.end()) {
      return std::nullopt;
    } else {
      return std::optional<AtomKey>(*key_iterator);
    }
  }

  void insert_key(const AtomKey& key) {
    if (!de_dup_map_) {
      util::raise_rte("Invalid de dup map object");
    }
    if (de_dup_map_->count(key.content_hash()) != 0)
      de_dup_map_->at(key.content_hash()).push_back(key);
    else
      de_dup_map_->insert({key.content_hash(), std::vector<AtomKey>({key})});
  }

private:
  std::unique_ptr<std::unordered_map<ContentHash, std::vector<AtomKey>>> de_dup_map_;
};

} // namespace arcticdb