/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/mongo/mongo_client.hpp>

#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/uri.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/mongo/mongo_instance.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/pool.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <mongocxx/model/replace_one.hpp>
#include <mongocxx/config/version.hpp>
#include <arcticdb/util/composite.hpp>

namespace arcticdb::storage::mongo {

namespace detail {

template<typename ElementType>
std::string get_string_element(const ElementType& element) {
#if (MONGOCXX_VERSION_MAJOR * 1000 + MONGOCXX_VERSION_MINOR) >= 3007
    return bsoncxx::string::to_string(element.get_string().value);
#else
    return element.get_utf8().value.data();
#endif
}

template<typename DocType>
StreamId stream_id_from_document(DocType& doc, KeyType key_type) {
    StreamId stream_id;
    if (is_string_key_type(key_type))
        stream_id = get_string_element(doc["stream_id"]);
    else
        stream_id = NumericId(doc["stream_id"].get_int64().value);

    return stream_id;
}

template<typename DocType>
AtomKey atom_key_from_document(DocType &doc, KeyType key_type) {
    auto index_type = IndexDescriptor::Type(doc["index_type"].get_int32().value);
    IndexValue start_index, end_index;
    if (index_type == IndexDescriptor::TIMESTAMP) {
        start_index = doc["start_time"].get_int64().value;
        end_index = doc["end_time"].get_int64().value;
    } else {
        start_index = get_string_element(doc["start_key"]);
        end_index = get_string_element(doc["end_key"]);
    }

    auto stream_id = stream_id_from_document(doc, key_type);

    return AtomKeyBuilder()
        .gen_id(doc["version_id"].get_int64().value)
        .creation_ts(timestamp(doc["creation_ts"].get_int64().value))
        .content_hash(doc["content_hash"].get_int64().value)
        .start_index(start_index)
        .end_index(end_index)
        .build(stream_id, key_type);
}

template<typename DocType>
RefKey ref_key_from_document(DocType &doc, KeyType key_type) {
    auto stream_id = stream_id_from_document(doc, key_type);
    bool is_old_type = key_type == KeyType::VERSION;
    return RefKey{ stream_id, key_type, is_old_type};
}

template<typename DocType>
entity::VariantKey variant_key_from_document(DocType& doc, const VariantKey& key) {
    auto key_type = variant_key_type(key);
    if(std::holds_alternative<AtomKey>(key)) {
        return detail::atom_key_from_document(doc, key_type);
    }
    else {
        return detail::ref_key_from_document(doc,key_type);
    }
}

template <typename KeyType>
void add_common_key_values(bsoncxx::builder::basic::document& basic_builder, const KeyType& key) {
    using namespace bsoncxx::builder::basic;
    using namespace mongocxx;
    using namespace bsoncxx;

    basic_builder.append(kvp("key_type", types::b_int32{static_cast<int32_t>(key.type())}));
    basic_builder.append(kvp("key", fmt::format("{}", key).c_str()));

    if(std::holds_alternative<std::string>(key.id()))
        basic_builder.append(kvp("stream_id", std::get<StringId>(key.id())));
    else
        basic_builder.append(kvp("stream_id", types::b_int64{int64_t(std::get<NumericId>(key.id()))}));
}

void add_atom_key_values(bsoncxx::builder::basic::document& basic_builder, const AtomKey& key) {
    using namespace bsoncxx::builder::basic;
    using namespace mongocxx;
    using namespace bsoncxx;
    using builder::stream::document;

    basic_builder.append(kvp("version_id",types::b_int64{int64_t(key.version_id())}));
    basic_builder.append(kvp("creation_ts", types::b_int64{int64_t(key.creation_ts())}));
    basic_builder.append(kvp("content_hash", types::b_int64{int64_t(key.content_hash())}));


    auto index_type = arcticdb::stream::get_index_value_type(key);
    basic_builder.append(kvp("index_type", types::b_int32{static_cast<int32_t>(index_type)}));
    if(index_type == IndexDescriptor::TIMESTAMP) {
        basic_builder.append(kvp("start_time", types::b_int64{int64_t(std::get<NumericId>(key.start_index()))}));
        basic_builder.append(kvp("end_time", types::b_int64{int64_t(std::get<NumericId>(key.end_index()))}));
    } else
    {
        basic_builder.append(kvp("start_key", std::get<StringId>(key.start_index())));
        basic_builder.append(kvp("end_key", std::get<StringId>(key.end_index())));
    }
}

auto build_document(storage::KeySegmentPair &kv) {
    using namespace bsoncxx::builder::basic;
    using namespace mongocxx;
    using namespace bsoncxx;
    using builder::stream::document;

    const auto &key = kv.variant_key();
    const auto &segment = kv.segment();
    const auto hdr_size = segment.segment_header_bytes_size();
    const auto total_size = segment.total_segment_size(hdr_size);
    /*thread_local*/ std::vector<uint8_t> buffer{};
    buffer.resize(total_size);
    bsoncxx::types::b_binary data = {};
    kv.segment().write_to(buffer.data(), hdr_size);
    data.size = uint32_t(total_size);
    data.bytes = buffer.data();

    bsoncxx::builder::basic::document basic_builder{};
    std::visit([&] (const auto& k) { add_common_key_values(basic_builder, k); }, key);
    if(std::holds_alternative<AtomKey>(key)) {
        add_atom_key_values(basic_builder, std::get<AtomKey>(key));
    }

    basic_builder.append(kvp("total_size", types::b_int64{int64_t(total_size)}));
    basic_builder.append(kvp("data", types::b_binary{data}));

    return basic_builder.extract();
}
} //namespace detail

class MongoClientImpl {
    using Config = arcticdb::proto::mongo_storage::Config;

    std::string get_connection_string(
        std::string uri,
        uint64_t min_pool_size,
        uint64_t max_pool_size,
        uint64_t selection_timeout_ms) {
        const auto uri_options = mongocxx::uri(uri).options();
        if (uri_options.find("minPoolSize") == uri_options.end())
            uri += fmt::format("&minPoolSize={}", min_pool_size);
        if (uri_options.find("maxPoolSize") == uri_options.end())
            uri += fmt::format("&maxPoolSize={}", max_pool_size);
        if (uri_options.find("serverSelectionTimeoutMS") == uri_options.end())
            uri += fmt::format("&serverSelectionTimeoutMS={}", selection_timeout_ms);
        return uri;
    }

  public:
    explicit MongoClientImpl(
        const Config& config,
        uint64_t min_pool_size,
        uint64_t max_pool_size,
        uint64_t selection_timeout_ms
        ) :
        connection_string_(get_connection_string(config.uri(), min_pool_size, max_pool_size, selection_timeout_ms)),
        pool_(mongocxx::uri(connection_string_)){
}

    void write_segment(
        const std::string &database_name,
        const std::string &collection_name,
        storage::KeySegmentPair&& kv);

    void update_segment(
        const std::string &database_name,
        const std::string &collection_name,
        storage::KeySegmentPair&& kv,
        bool upsert);

    storage::KeySegmentPair read_segment(
        const std::string &database_name,
        const std::string &collection_name,
        const  entity::VariantKey &key);

    void remove_keyvalue(
        const std::string &database_name,
        const std::string &collection_name,
        const  entity::VariantKey &key);

    void iterate_type(
        const std::string &database_name,
        const std::string &collection_name,
        KeyType key_type,
        folly::Function<void(entity::VariantKey &&)>&& visitor,
        const std::optional<std::string> &prefix
    );

    void ensure_collection(
        std::string_view database_name,
        std::string_view collection_name);

    void drop_collection(
            std::string database_name,
            std::string collection_name);

    bool key_exists(const std::string &database_name,
                                     const std::string &collection_name,
                                     const  entity::VariantKey &key);

    MongoClientImpl(const MongoClientImpl&) = delete;
    MongoClientImpl(MongoClientImpl&&) = delete;
    MongoClientImpl& operator=(const MongoClientImpl&) = delete;
    MongoClientImpl& operator=(MongoClientImpl&&) = delete;

  private:
    auto get_client() {
        auto instance = MongoInstance::instance();

        auto try_get = [&]() {
            return pool_.acquire();
        };

        auto client = ExponentialBackoff<std::runtime_error>(100, 2000).go(std::move(try_get));
        util::check(bool(client), "Pool did not return a client");
        return client;
    }

    std::string connection_string_;
    mongocxx::pool pool_;
};

void MongoClientImpl::write_segment(const std::string &database_name,
                                    const std::string &collection_name,
                                    storage::KeySegmentPair &&kv) {
    using namespace bsoncxx::builder::stream;
    using bsoncxx::builder::stream::document;
    ARCTICDB_SUBSAMPLE(MongoStorageWriteGetClient, 0)
    auto client = get_client();

    ARCTICDB_SUBSAMPLE(MongoStorageWriteBuildDoc, 0)
    auto doc = detail::build_document(kv);

    ARCTICDB_SUBSAMPLE(MongoStorageWriteGetCol, 0)
    mongocxx::database database = client->database(database_name.c_str());
    auto collection = database[collection_name];

    ARCTICDB_SUBSAMPLE(MongoStorageWriteInsertOne, 0)
    if(std::holds_alternative<RefKey>(kv.variant_key())) {
        mongocxx::model::replace_one replace{document{} << "key" << fmt::format("{}", kv.ref_key()) << finalize, doc.view()};
        replace.upsert(true);
        auto bulk_write = collection.create_bulk_write();
        bulk_write.append(replace);
        auto result = bulk_write.execute();
        util::check(bool(result), "Mongo error while putting key {}", kv.key_view());
    } else {
        auto result = collection.insert_one(doc.view());
        util::check(bool(result), "Mongo error while putting key {}", kv.key_view());
    }
}

void MongoClientImpl::update_segment(const std::string &database_name,
                                    const std::string &collection_name,
                                    storage::KeySegmentPair &&kv,
                                    bool upsert) {
    using namespace bsoncxx::builder::stream;
    using bsoncxx::builder::stream::document;
    ARCTICDB_SUBSAMPLE(MongoStorageUpdateGetClient, 0)
    auto client = get_client();

    ARCTICDB_SUBSAMPLE(MongoStorageUpdateBuildDoc, 0)
    auto doc = detail::build_document(kv);

    ARCTICDB_SUBSAMPLE(MongoStorageUpdateGetCol, 0)
    mongocxx::database database = client->database(database_name.c_str());
    auto collection = database[collection_name];

    ARCTICDB_SUBSAMPLE(MongoStorageUpdateInsertOne, 0)
    mongocxx::model::replace_one replace{document{} << "key" << fmt::format("{}", kv.variant_key()) << finalize, doc.view()};
    replace.upsert(upsert);
    auto bulk_write = collection.create_bulk_write();
    bulk_write.append(replace);
    auto result = bulk_write.execute();
    util::check(bool(result), "Mongo error while updating key {}", kv.key_view());
    util::check(upsert || result->modified_count() > 0, "update called with upsert=false but key does not exist");
}

storage::KeySegmentPair MongoClientImpl::read_segment(const std::string &database_name,
                                                   const std::string &collection_name,
                                                   const  entity::VariantKey &key) {
    using namespace bsoncxx::builder::stream;
    using bsoncxx::builder::stream::document;
    ARCTICDB_SUBSAMPLE(MongoStorageReadGetClient, 0)

    auto client = get_client();

    ARCTICDB_SUBSAMPLE(MongoStorageReadGetCol, 0)
    auto database = client->database(database_name); //TODO maybe cache
    auto collection = database[collection_name];

    try {
        ARCTICDB_SUBSAMPLE(MongoStorageReadFindOne, 0)
        auto stream_id = variant_key_id(key);
        if(StorageFailureSimulator::instance()->configured())
            StorageFailureSimulator::instance()->go(FailureType::READ);

        auto result = collection.find_one(document{} << "key" << fmt::format("{}", key) << "stream_id" <<
                                                                                                       fmt::format("{}", stream_id) << finalize);
        if (result) {
            const auto &doc = result->view();
            auto size = doc["total_size"].get_int64().value;
            entity::VariantKey stored_key{ detail::variant_key_from_document(doc, key) };
            util::check(stored_key == key, "Key mismatch: {} != {}");
            return storage::KeySegmentPair(
                    std::move(stored_key),
                    Segment::from_bytes(const_cast<uint8_t *>(result->view()["data"].get_binary().bytes), std::size_t(size), true)
            );
        } else {
            // find_one returned nothing, if this was an exception it would fall through to the catch below.
            throw std::runtime_error(fmt::format("Missing key in mongo: {} for symbol: {}", key, stream_id));
        }
    }
    catch(const std::exception& ex) {
        log::storage().info("Segment read error: {}", ex.what());
        throw storage::KeyNotFoundException{Composite<VariantKey>{VariantKey{key}}};
    }
}

bool MongoClientImpl::key_exists(const std::string &database_name,
                                                      const std::string &collection_name,
                                                      const  entity::VariantKey &key) {
    using namespace bsoncxx::builder::stream;
    using bsoncxx::builder::stream::document;
    ARCTICDB_SUBSAMPLE(MongoStorageReadGetClient, 0)

    auto client = get_client();

    ARCTICDB_SUBSAMPLE(MongoStorageKeyExists, 0)
    auto database = client->database(database_name); //TODO maybe cache
    auto collection = database[collection_name];

    try {
        ARCTICDB_SUBSAMPLE(MongoStorageKeyExistsFindOne, 0)
        auto result = collection.find_one(document{} << "key" << fmt::format("{}", key) << finalize);
        return static_cast<bool>(result);
    }
    catch(const std::exception& ex) {
        log::storage().error(fmt::format("Key exists error: {}", ex.what()));
        throw;
    }
}


void MongoClientImpl::remove_keyvalue(const std::string &database_name,
                                                 const std::string &collection_name,
                                                 const entity::VariantKey &key) {
    using namespace bsoncxx::builder::stream;
    using bsoncxx::builder::stream::document;
    ARCTICDB_SUBSAMPLE(MongoStorageRemoveGetClient, 0)

    auto client = get_client();
    auto database = client->database(database_name); //TODO cache
    auto collection = database[collection_name];
    ARCTICDB_SUBSAMPLE(MongoStorageRemoveGetCol, 0)
    mongocxx::stdx::optional<mongocxx::result::delete_result> result;
    if (std::holds_alternative<RefKey>(key)) {
        result = collection.delete_many(document{} << "key" << fmt::format("{}", key) << "stream_id" <<
                                                   fmt::format("{}", variant_key_id(key)) << finalize);
    } else {
        result = collection.delete_one(document{} << "key" << fmt::format("{}", key) << "stream_id" <<
                                                  fmt::format("{}", variant_key_id(key)) << finalize);
    }
    ARCTICDB_SUBSAMPLE(MongoStorageRemoveDelOne, 0)
    if (result) {
        std::int32_t deleted_count = result->deleted_count();
        util::warn(deleted_count == 1, "Expect to delete a single document with key {}",
                   key); // possible values are 0 and 1 here when returned from delete_one
    } else
        throw std::runtime_error(fmt::format("Mongo error deleting data for key {}", key));

}

void MongoClientImpl::iterate_type(const std::string &database_name,
                               const std::string &collection_name,
                               KeyType key_type,
                               folly::Function<void(entity::VariantKey &&)>&& visitor,
                               const std::optional<std::string> &prefix
                               ) {
    using namespace bsoncxx::builder::stream;
    using bsoncxx::builder::stream::document;
    ARCTICDB_SUBSAMPLE(MongoStorageItTypeGetClient, 0)
    auto client = get_client();
    ARCTICDB_SUBSAMPLE(MongoStorageItTypeGetCol, 0)
    auto collection = client->database(database_name)[collection_name];
    ARCTICDB_SUBSAMPLE(MongoStorageItTypeFindAll, 0)
    bool has_prefix = prefix.has_value() && (!prefix.value().empty());
    auto cursor =  has_prefix ?
            collection.find(document{} << "stream_id" << prefix.value() << finalize):
            collection.find({});

    for (auto &doc : cursor) {
        if(!is_ref_key_class(key_type)) {
            auto key = detail::atom_key_from_document(doc, key_type);
            visitor(std::move(key));
        }
        else {
            auto key = detail::ref_key_from_document(doc, key_type);
            visitor(std::move(key));
        }
        ARCTICDB_SUBSAMPLE(MongoStorageItTypeNext, 0)
    }
}

void MongoClientImpl::ensure_collection(std::string_view database_name, std::string_view collection_name) {
    using bsoncxx::builder::stream::document;
    using bsoncxx::builder::stream::finalize;
    auto client = get_client();
    auto database = client->database(database_name.data());
    auto col = database.create_collection(collection_name.data());
    auto index_specification = document{} << "key" << 1 << finalize;
    col.create_index(std::move(index_specification));
}

void MongoClientImpl::drop_collection(std::string database_name, std::string collection_name) {
    using bsoncxx::builder::stream::document;
    using bsoncxx::builder::stream::finalize;

    auto client = get_client();
    try {
        auto collection = client->database(database_name)[collection_name];
        collection.drop();
    } catch (const std::exception &e) {
        log::storage().info("Got an exception from Mongo: {} when trying to delete: {}:{}",
                            e.what(), database_name, collection_name);
    }
}

/*
 * Pimpl idiom hides mongo headers from python code, and avoids problems with Mongo Cxx's
 * rather promiscuous namespace usage.
 */
MongoClient::MongoClient(
    const Config& config,
    uint64_t min_pool_size,
    uint64_t max_pool_size,
    uint64_t selection_timeout_ms) :
    client_(new MongoClientImpl(config, min_pool_size, max_pool_size, selection_timeout_ms)) {}

MongoClient::~MongoClient() {
    delete client_;
}

void MongoClient::write_segment(const std::string &database_name,
                                 const std::string &collection_name,
                                 storage::KeySegmentPair &&kv) {
    client_->write_segment(database_name, collection_name, std::move(kv));
}

void MongoClient::update_segment(const std::string &database_name,
                                const std::string &collection_name,
                                storage::KeySegmentPair &&kv,
                                bool upsert) {
    client_->update_segment(database_name, collection_name, std::move(kv), upsert);
}

storage::KeySegmentPair MongoClient::read_segment(const std::string &database_name,
                                             const std::string &collection_name,
                                             const entity::VariantKey &key) {
    return client_->read_segment(database_name, collection_name, key);
}

void MongoClient::remove_keyvalue(const std::string &database_name,
                                  const std::string &collection_name,
                                  const entity::VariantKey &key) {
    client_->remove_keyvalue(database_name, collection_name, key);
}

void MongoClient::iterate_type(const std::string &database_name,
                               const std::string &collection_name,
                               KeyType key_type,
                               folly::Function<void(entity::VariantKey &&)>&& visitor,
                               const std::optional<std::string> &prefix
                               ) {
    client_->iterate_type(database_name, collection_name, key_type, std::move(visitor), prefix);
}

void MongoClient::ensure_collection(std::string_view database_name, std::string_view collection_name) {
    client_->ensure_collection(database_name, collection_name);
}

void MongoClient::drop_collection(std::string database_name, std::string collection_name) {
    client_->drop_collection(database_name, collection_name);
}

bool MongoClient::key_exists(const std::string &database_name,
                                 const std::string &collection_name,
                                 const  entity::VariantKey &key) {
    return client_->key_exists(database_name, collection_name, key);
}

} //namespace arcticdb::storage::mongo
