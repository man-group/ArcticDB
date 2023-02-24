/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/variant.hpp>


namespace arcticdb {

using namespace arcticdb::entity;

inline arcticdb::proto::descriptors::AtomKey encode_key(const AtomKey &key) {
    arcticdb::proto::descriptors::AtomKey output;
    util::variant_match(key.id(),
                        [&](const StringId &id) { output.set_string_id(id); },
                        [&](const NumericId &id) { output.set_numeric_id(id); });
    output.set_version_id(key.version_id());
    output.set_creation_ts(key.creation_ts());
    output.set_content_hash(key.content_hash());

    util::variant_match(key.start_index(),
                        [&](const StringId &id) { output.set_string_start(id); },
                        [&](const NumericId &id) { output.set_numeric_start(id); });
    util::variant_match(key.end_index(),
                        [&](const StringId &id) { output.set_string_end(id); },
                        [&](const NumericId &id) { output.set_numeric_end(id); });

    output.set_key_type(arcticdb::proto::descriptors::KeyType (int(key.type())));
    return output;
}

inline AtomKey decode_key(const arcticdb::proto::descriptors::AtomKey& input) {
    StreamId stream_id = input.id_case() == input.kNumericId ? StreamId(input.numeric_id()) : StreamId(input.string_id());
    IndexValue index_start = input.index_start_case() == input.kNumericStart ? IndexValue(input.numeric_start()) : IndexValue(input.string_start());
    IndexValue index_end = input.index_end_case() == input.kNumericEnd ? IndexValue(input.numeric_end() ): IndexValue(input.string_end());

    return atom_key_builder()
         .version_id(input.version_id())
         .creation_ts(timestamp(input.creation_ts()))
         .content_hash(input.content_hash())
         .start_index(index_start)
         .end_index(index_end)
         .build(stream_id, KeyType(input.key_type()));
}

} //namespace arcticdb