/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/component_manager.hpp>

namespace arcticdb {

void ComponentManager::set_next_entity_id(EntityId id) { next_entity_id_ = id; }

EntityId ComponentManager::entity_id(const std::optional<EntityId>& id) {
  // Do not use value_or as we do not want the side effect of fetch_add when id was
  // provided by the caller
  return id.has_value() ? *id : next_entity_id_.fetch_add(1);
}

} // namespace arcticdb