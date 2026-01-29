/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/mock/s3_mock_client.hpp>
#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_client_impl.hpp>
#include <arcticdb/util/simple_string_hash.hpp>
#include <arcticdb/storage/s3/detail-inl.hpp>
namespace arcticdb::storage::nfs_backed {

std::string add_suffix_char(const std::string& str) { return fmt::format("{}*", str); }

std::string remove_suffix_char(const std::string& str) {
    util::check(!str.empty() && *str.rbegin() == '*', "Unexpected string passed to remove_suffix_char: {}", str);
    return str.substr(0, str.size() - 1);
}

template<typename MixedType, typename StringType, typename NumericType>
MixedType encode_item(const MixedType& input, bool add_suffix) {
    return util::variant_match(
            input,
            [add_suffix](const StringType& str) {
                const auto encoded = util::safe_encode(str);
                return add_suffix ? MixedType{add_suffix_char(encoded)} : MixedType{encoded};
            },
            [](const NumericType& id) { return MixedType{id}; }
    );
}

template<typename MixedType, typename StringType, typename NumericType>
MixedType decode_item(const MixedType& input, bool remove_suffix) {
    return util::variant_match(
            input,
            [remove_suffix](const StringType& str) {
                return MixedType{util::safe_decode(remove_suffix ? remove_suffix_char(str) : str)};
            },
            [](const NumericType& id) { return MixedType{id}; }
    );
}

VariantKey encode_object_id(const VariantKey& key) {
    return util::variant_match(
            key,
            [](const AtomKey& k) {
                auto encoded_id = encode_item<StreamId, StringId, NumericId>(k.id(), false);
                auto start_index = encode_item<IndexValue, StringIndex, NumericIndex>(k.start_index(), false);
                auto end_index = encode_item<IndexValue, StringIndex, NumericIndex>(k.end_index(), false);
                return VariantKey{atom_key_builder()
                                          .version_id(k.version_id())
                                          .start_index(start_index)
                                          .end_index(end_index)
                                          .creation_ts(k.creation_ts())
                                          .content_hash(k.content_hash())
                                          .build(encoded_id, k.type())};
            },
            [](const RefKey& r) {
                auto encoded_id = encode_item<StreamId, StringId, NumericId>(r.id(), true);
                return VariantKey{RefKey{encoded_id, r.type(), r.is_old_type()}};
            }
    );
}

uint32_t id_to_number(const StreamId& stream_id) {
    return util::variant_match(
            stream_id,
            [](const NumericId& num_id) { return static_cast<uint32_t>(num_id); },
            [](const StringId& str_id) { return murmur3_32(str_id); }
    );
}

uint32_t get_id_bucket(const StreamId& id) { return id_to_number(id) % 1000; }

uint32_t get_hash_bucket(const AtomKey& atom) { return atom.content_hash() % 1000; }

std::string get_root_folder(const std::string& root_folder, const RefKey& ref) {
    const auto id_bucket = get_id_bucket(ref.id());
    return fmt::format("{}/{:03}", root_folder, id_bucket);
}

std::string get_root_folder(const std::string& root_folder, const AtomKey& atom) {
    const auto id_bucket = get_id_bucket(atom.id());
    const auto hash_bucket = get_hash_bucket(atom);
    return fmt::format("{}/{:03}/{:03}", root_folder, id_bucket, hash_bucket);
}

std::string get_root_folder(const std::string& root_folder, const VariantKey& vk) {
    return util::variant_match(vk, [&root_folder](const auto& k) { return get_root_folder(root_folder, k); });
}

std::string NfsBucketizer::bucketize(const std::string& root_folder, const VariantKey& vk) {
    return get_root_folder(root_folder, vk);
}

size_t NfsBucketizer::bucketize_length(KeyType key_type) { return is_ref_key_class(key_type) ? 4 : 8; }

VariantKey unencode_object_id(const VariantKey& key) {
    return util::variant_match(
            key,
            [](const AtomKey& k) {
                auto decoded_id = decode_item<StreamId, StringId, NumericId>(k.id(), false);
                auto start_index = decode_item<IndexValue, StringIndex, NumericIndex>(k.start_index(), false);
                auto end_index = decode_item<IndexValue, StringIndex, NumericIndex>(k.end_index(), false);
                return VariantKey{atom_key_builder()
                                          .version_id(k.version_id())
                                          .start_index(start_index)
                                          .end_index(end_index)
                                          .creation_ts(k.creation_ts())
                                          .content_hash(k.content_hash())
                                          .build(decoded_id, k.type())};
            },
            [](const RefKey& r) {
                auto decoded_id = decode_item<StreamId, StringId, NumericId>(r.id(), true);
                return VariantKey{RefKey{decoded_id, r.type(), r.is_old_type()}};
            }
    );
}

NfsBackedStorage::NfsBackedStorage(const LibraryPath& library_path, OpenMode mode, const Config& conf) :
    Storage(library_path, mode),
    s3_api_(s3::S3ApiInstance::instance()), // make sure we have an initialized AWS SDK
    root_folder_(object_store_utils::get_root_folder(library_path)),
    bucket_name_(conf.bucket_name()),
    region_(conf.region()) {

    if (conf.use_mock_storage_for_testing()) {
        log::storage().warn("Using Mock S3 storage for NfsBackedStorage");
        s3_client_ = std::make_unique<s3::S3ClientTestWrapper>(std::make_unique<s3::MockS3Client>());
    } else {
        s3_client_ = std::make_unique<s3::S3ClientImpl>(
                s3::get_aws_credentials(conf),
                s3::get_s3_config_and_set_env_var(conf),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false
        );
    }

    if (conf.prefix().empty()) {
        ARCTICDB_DEBUG(log::version(), "prefix not found, will use {}", root_folder_);
    } else if (conf.use_raw_prefix()) {
        ARCTICDB_DEBUG(log::version(), "raw prefix found, using: {}", conf.prefix());
        root_folder_ = conf.prefix();
    } else {
        auto prefix_path = LibraryPath::from_delim_path(conf.prefix(), '.');
        root_folder_ = object_store_utils::get_root_folder(prefix_path);
        ARCTICDB_DEBUG(log::version(), "parsed prefix found, using: {}", root_folder_);
    }

    // When linking against libraries built with pre-GCC5 compilers, the num_put facet is not initalized on the classic
    // locale Rather than change the locale globally, which might cause unexpected behaviour in legacy code, just add
    // the required facet here
    std::locale locale{std::locale::classic(), new std::num_put<char>()};
    (void)std::locale::global(locale);
    ARCTICDB_DEBUG(log::storage(), "Opened NFS backed storage at {}", root_folder_);
}

std::string NfsBackedStorage::name() const {
    return fmt::format("nfs_backed_storage-{}/{}/{}", region_, bucket_name_, root_folder_);
}

std::string NfsBackedStorage::do_key_path(const VariantKey& key) const {
    auto b = NfsBucketizer{};
    auto key_type_dir = key_type_folder(root_folder_, variant_key_type(key));
    return object_path(b.bucketize(key_type_dir, key), key);
}

void NfsBackedStorage::do_write(KeySegmentPair& key_seg) {
    auto enc = KeySegmentPair{encode_object_id(key_seg.variant_key()), key_seg.segment_ptr()};
    s3::detail::do_write_impl(enc, root_folder_, bucket_name_, *s3_client_, NfsBucketizer{});
}

void NfsBackedStorage::do_update(KeySegmentPair& key_seg, UpdateOpts) {
    auto enc = KeySegmentPair{encode_object_id(key_seg.variant_key()), key_seg.segment_ptr()};
    s3::detail::do_update_impl(enc, root_folder_, bucket_name_, *s3_client_, NfsBucketizer{});
}

void NfsBackedStorage::do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) {
    auto encoded_key = encode_object_id(variant_key);
    auto decoder = [](auto&& k) { return unencode_object_id(std::move(k)); };
    s3::detail::do_read_impl(
            std::move(variant_key),
            visitor,
            root_folder_,
            bucket_name_,
            *s3_client_,
            NfsBucketizer{},
            std::move(decoder),
            opts
    );
}

KeySegmentPair NfsBackedStorage::do_read(VariantKey&& variant_key, ReadKeyOpts opts) {
    auto encoded_key = encode_object_id(variant_key);
    auto decoder = [](auto&& k) { return unencode_object_id(std::move(k)); };
    return s3::detail::do_read_impl(
            std::move(encoded_key), root_folder_, bucket_name_, *s3_client_, NfsBucketizer{}, std::move(decoder), opts
    );
}

void NfsBackedStorage::do_remove(VariantKey&& variant_key, RemoveOpts) {
    auto enc = encode_object_id(variant_key);
    std::array<VariantKey, 1> arr{std::move(enc)};
    s3::detail::do_remove_impl(std::span(arr), root_folder_, bucket_name_, *s3_client_, NfsBucketizer{});
}

void NfsBackedStorage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts) {
    std::vector<VariantKey> enc;
    enc.reserve(variant_keys.size());
    std::transform(std::begin(variant_keys), std::end(variant_keys), std::back_inserter(enc), [](auto&& key) {
        return encode_object_id(key);
    });
    s3::detail::do_remove_impl(std::span(enc), root_folder_, bucket_name_, *s3_client_, NfsBucketizer{});
}

bool NfsBackedStorage::do_iterate_type_until_match(
        KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix
) {
    auto path_info = s3::detail::calculate_path_info(
            root_folder_, key_type, false, prefix, NfsBucketizer::bucketize_length(key_type)
    );
    // We need this prefix filtering here instead of a regex like in s3/azure
    // because we are doing sharding through subdirectories
    // and the prefix might be partial(e.g. "sym_" instead of "sym_123")
    // so it cannot be hashed to the correct shard
    const IterateTypePredicate visitor_with_prefix_filtering = [&visitor, &prefix](VariantKey&& key) {
        return s3::detail::visit_if_prefix_matches(visitor, prefix, unencode_object_id(key));
    };
    s3::detail::Visitor<IterateTypePredicate> final_visitor{visitor_with_prefix_filtering};
    return s3::detail::do_iterate_type_impl(key_type, bucket_name_, *s3_client_, path_info, final_visitor);
}

bool NfsBackedStorage::do_key_exists(const VariantKey& key) {
    auto encoded_key = encode_object_id(key);
    return s3::detail::do_key_exists_impl(encoded_key, root_folder_, bucket_name_, *s3_client_, NfsBucketizer{});
}

bool NfsBackedStorage::supports_object_size_calculation() const { return true; }

void NfsBackedStorage::do_visit_object_sizes(
        KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor
) {
    auto path_info = s3::detail::calculate_path_info(
            root_folder_, key_type, false, prefix, NfsBucketizer::bucketize_length(key_type)
    );
    // We need this prefix filtering here instead of a regex like in s3/azure
    // because we are doing sharding through subdirectories
    // and the prefix might be partial(e.g. "sym_" instead of "sym_123")
    // so it cannot be hashed to the correct shard
    const ObjectSizesVisitor visitor_with_prefix_filtering = [&visitor,
                                                              &prefix](const VariantKey& key, CompressedSize size) {
        s3::detail::object_sizes_visit_if_prefix_matches(visitor, prefix, unencode_object_id(key), size);
    };
    s3::detail::Visitor<ObjectSizesVisitor> final_visitor{visitor_with_prefix_filtering};

    s3::detail::do_visit_object_sizes_for_type_impl(key_type, bucket_name_, *s3_client_, path_info, final_visitor);
}

} // namespace arcticdb::storage::nfs_backed
