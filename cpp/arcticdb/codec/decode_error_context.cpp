/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/decode_error_context.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

void rethrow_with_context(const DataIntegrityException& e, std::string_view context) {
    throw DataIntegrityException(fmt::format("{}, in {}", e.what(), context));
}

void rethrow_with_context(const CodecException& e, std::string_view context) {
    throw CodecException(fmt::format("{}, in {}", e.what(), context));
}

std::string describe_key_for_decode_error(const entity::VariantKey& key, bool is_hash_mismatch) {
    const auto version = util::variant_match(
            key,
            [](const entity::AtomKey& atom_key) { return fmt::format(", version {}", atom_key.version_id()); },
            [](const entity::RefKey&) { return std::string{}; }
    );
    return fmt::format(
            "key {} (symbol '{}'{}, storage object name '{}'). {} don't match what was written. If re-reading "
            "succeeds, the damage happened in transit (consider HTTPS or S3 response checksums); if it persists, the "
            "stored object is corrupt.",
            key,
            variant_key_id(key),
            version,
            to_tokenized_key(key),
            is_hash_mismatch ? "The bytes read from storage"
                             : "If this object was written by this or an older ArcticDB version, the bytes read from "
                               "storage"
    );
}

} // namespace arcticdb
