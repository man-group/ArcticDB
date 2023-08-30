/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_NFS_BACKED_STORAGE_H_
#error "This should only be included by nfs_backed_storage.cpp"
#endif

#include <arcticdb/util/simple_string_hash.hpp>
#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>

namespace arcticdb::storage::nfs_backed {

inline std::string add_suffix_char(const std::string& str) {
    return fmt::format("{}*", str);
}

inline std::string remove_suffix_char(const std::string& str) {
    return str.substr(0, str.size()-1);
}

template<typename MixedType, typename StringType, typename NumericType>
MixedType encode_item(const MixedType& input, bool add_suffix) {
    return util::variant_match(input,
    [add_suffix] (const StringType& str) {
        const auto encoded = util::safe_encode(str);
        return add_suffix ? MixedType{add_suffix_char(encoded)} : MixedType{encoded};
    },
    [](const NumericType& id) {
        return MixedType{id};
    });
}

template<typename MixedType, typename StringType, typename NumericType>
MixedType decode_item(const MixedType& input, bool remove_suffix) {
    return util::variant_match(input,
    [remove_suffix] (const StringType& str) {
        return MixedType{util::safe_decode(remove_suffix ? remove_suffix_char(str) : str)};
    },
    [](const NumericType& id) {
       return MixedType{id};
    });
}

inline VariantKey encode_object_id(const VariantKey& key) {
    return util::variant_match(key,
        [] (const AtomKey& k) {
            auto encoded_id = encode_item<StreamId, StringId, NumericId>(k.id(), false);
            auto start_index = encode_item<IndexValue, StringIndex, NumericIndex>(k.start_index(), false);
            auto end_index = encode_item<IndexValue, StringIndex, NumericIndex>(k.end_index(), false);
            return VariantKey{atom_key_builder().version_id(k.version_id()).start_index(start_index)
            .end_index(end_index).creation_ts(k.creation_ts()).content_hash(k.content_hash())
            .build(encoded_id, k.type())};
    },
        [](const RefKey& r) {
        auto encoded_id = encode_item<StreamId, StringId, NumericId>(r.id(), true);
        return VariantKey{RefKey{encoded_id, r.type(), r.is_old_type()}};
    });
}

inline uint32_t id_to_number(const StreamId& stream_id) {
    return util::variant_match(stream_id,
       [] (const NumericId& num_id) { return static_cast<uint32_t>(num_id); },
       [] (const StringId& str_id) { return murmur3_32(str_id); });
}

inline uint32_t get_id_bucket(const StreamId& id) {
    return id_to_number(id) % 1000;
}

inline uint32_t get_hash_bucket(const AtomKey& atom) {
    return atom.content_hash() % 1000;
}

inline std::string get_root_folder(const std::string& root_folder, const RefKey& ref) {
    const auto id_bucket = get_id_bucket(ref.id());
    return fmt::format("{}/{:03}", root_folder, id_bucket);
}

inline std::string get_root_folder(const std::string& root_folder, const AtomKey& atom) {
    const auto id_bucket = get_id_bucket(atom.id());
    const auto hash_bucket = get_hash_bucket(atom);
    return fmt::format("{}/{:03}/{:03}", root_folder, id_bucket, hash_bucket);
}

inline std::string get_root_folder(const std::string& root_folder, const VariantKey& vk) {
    return util::variant_match(vk, [&root_folder] (const auto& k) {
        return get_root_folder(root_folder, k);
    });
}

struct NfsBucketizer {
    static std::string bucketize(const std::string& root_folder, const VariantKey& vk) {
        return get_root_folder(root_folder, vk);
    }

    static size_t bucketize_length(KeyType key_type) {
        return is_ref_key_class(key_type) ? 4 : 8;
    }
};

inline VariantKey unencode_object_id(const VariantKey& key) {

    return util::variant_match(key,
                        [] (const AtomKey& k) {
                            auto decoded_id = decode_item<StreamId, StringId, NumericId>(k.id(), false);
                            auto start_index = decode_item<IndexValue, StringIndex, NumericIndex>(k.start_index(), false);
                            auto end_index = decode_item<IndexValue, StringIndex, NumericIndex>(k.end_index(), false);
                            return VariantKey{atom_key_builder().version_id(k.version_id()).start_index(start_index)
                                .end_index(end_index).creation_ts(k.creation_ts()).content_hash(k.content_hash())
                                .build(decoded_id, k.type())};
                        },
                        [](const RefKey& r) {
                            auto decoded_id = decode_item<StreamId, StringId, NumericId>(r.id(), true);
                            return VariantKey{RefKey{decoded_id, r.type(), r.is_old_type()}};
                        });
}

inline void NfsBackedStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    auto enc = kvs.transform([] (auto&& key_seg) {
        return KeySegmentPair{encode_object_id(key_seg.variant_key()), std::move(key_seg.segment())};
    });
    s3::detail::do_write_impl(std::move(enc), root_folder_, bucket_name_, s3_client_, NfsBucketizer{});
}

inline void NfsBackedStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts) {
    auto enc = kvs.transform([] (auto&& key_seg) {
        return KeySegmentPair{encode_object_id(key_seg.variant_key()), std::move(key_seg.segment())};
    });
    s3::detail::do_update_impl(std::move(enc), root_folder_, bucket_name_, s3_client_, NfsBucketizer{});
}

inline void NfsBackedStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) {
    auto func = [visitor] (const VariantKey& k, Segment&& seg) mutable {
        visitor(unencode_object_id(k), std::move(seg));
    };

    auto enc = ks.transform([] (auto&& key) {
        return encode_object_id(key);
    });

    s3::detail::do_read_impl(std::move(enc), func, root_folder_, bucket_name_, s3_client_, NfsBucketizer{}, opts);
}

inline void NfsBackedStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts) {
    auto enc = ks.transform([] (auto&& key) {
        return encode_object_id(key);
    });
    s3::detail::do_remove_impl(std::move(enc), root_folder_, bucket_name_, s3_client_, NfsBucketizer{});
}

inline void NfsBackedStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string& prefix) {
    auto func = [v = std::move(visitor), prefix=prefix] (VariantKey&& k) mutable {
        auto key = unencode_object_id(k);
        if(prefix.empty() || variant_key_id(key) == StreamId{prefix})
            v(std::move(key));
    };

    auto prefix_handler = [] (const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor&, KeyType key_type) {
        std::string new_prefix;
        if(!prefix.empty()) {
            uint32_t id = get_id_bucket(encode_item<StreamId, StringId, NumericId>(StringId{prefix}, is_ref_key_class(key_type)));
            new_prefix = fmt::format("{:03}", id);
        }

        return !prefix.empty() ? fmt::format("{}/{}", key_type_dir, new_prefix) : key_type_dir;
    };

    s3::detail::do_iterate_type_impl(key_type, std::move(func), root_folder_, bucket_name_, s3_client_, NfsBucketizer{}, prefix_handler, prefix);
}

inline bool NfsBackedStorage::do_key_exists(const VariantKey& key) {
    auto encoded_key = encode_object_id(key);
    return s3::detail::do_key_exists_impl(encoded_key, root_folder_, bucket_name_, s3_client_, NfsBucketizer{});
}

} //namespace arcticdb::nfs_backed