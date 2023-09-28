/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/entity/key.hpp>

namespace arcticdb::entity {

struct KeyData {
    KeyData() :
        long_name_("undefined"),
        short_name_('u'),
        variant_type_(VariantType::UNKNOWN_TYPE),
        key_class_(KeyClass::UNKNOWN_CLASS),
        description_()
        {}

    KeyData(const char* long_name, char short_name, VariantType variant_type, KeyClass key_class, const char* description) :
        long_name_(long_name),
        short_name_(short_name),
        variant_type_(variant_type),
        key_class_(key_class),
        description_(description){}


    const char* long_name_;
    char short_name_;
    VariantType variant_type_;
    KeyClass key_class_;
    const char* description_;
};

KeyData get_key_data(KeyType key_type) {
#define NUMERIC_KEY(kt, name, c) case kt: return KeyData{ #name, c, VariantType::NUMERIC_TYPE, KeyClass::ATOM_KEY, #kt };
#define STRING_KEY(kt, name, c)  case kt: return KeyData{ #name, c, VariantType::STRING_TYPE, KeyClass::ATOM_KEY, #kt};
#define STRING_REF(kt, name, c)  case kt: return KeyData{ #name, c, VariantType::STRING_TYPE, KeyClass::REF_KEY,  #kt };

    switch (key_type) {
    // Important ones
    STRING_REF(KeyType::VERSION_REF, vref, 'r')
    STRING_KEY(KeyType::TABLE_DATA, tdata, 'd')
    STRING_KEY(KeyType::TABLE_INDEX, tindex, 'i')
    STRING_KEY(KeyType::VERSION, ver, 'V')
    STRING_KEY(KeyType::VERSION_JOURNAL, vj, 'v')
    STRING_KEY(KeyType::SNAPSHOT, snap, 's')
    STRING_KEY(KeyType::SYMBOL_LIST, sl, 'l')
    STRING_KEY(KeyType::TOMBSTONE_ALL, tall, 'q')
    STRING_KEY(KeyType::TOMBSTONE, tomb, 'x')
    STRING_REF(KeyType::LIBRARY_CONFIG, cref, 'C')
    STRING_KEY(KeyType::COLUMN_STATS, cstats, 'S')
    STRING_REF(KeyType::SNAPSHOT_REF, tref, 't')
    // Less important
    STRING_KEY(KeyType::LOG, log, 'o')
    STRING_KEY(KeyType::LOG_COMPACTED, logc, 'O')
    STRING_REF(KeyType::OFFSET, off, 'f')
    STRING_REF(KeyType::BACKUP_SNAPSHOT_REF, bref, 'B')
    STRING_KEY(KeyType::METRICS, met, 'M')
    STRING_REF(KeyType::APPEND_REF, aref, 'a')
    STRING_KEY(KeyType::MULTI_KEY, mref, 'm')
    STRING_REF(KeyType::LOCK, lref, 'x')
    STRING_REF(KeyType::SNAPSHOT_TOMBSTONE, ttomb, 'X')
    STRING_KEY(KeyType::APPEND_DATA, app, 'b')
    // Unused
    STRING_KEY(KeyType::PARTITION, pref, 'p')
    STRING_REF(KeyType::STORAGE_INFO, sref, 'h')
    NUMERIC_KEY(KeyType::STREAM_GROUP, sg, 'g')
    NUMERIC_KEY(KeyType::GENERATION, gen, 'G')
    default:util::raise_rte("Could not get data for key_type {}", static_cast<int>(key_type));
    };
}

const char* key_type_long_name(KeyType key_type) {
    return get_key_data(key_type).long_name_;
}

char key_type_short_name(KeyType key_type) {
    return get_key_data(key_type).short_name_;
}

VariantType variant_type_from_key_type(KeyType key_type) {
    return get_key_data(key_type).variant_type_;
}

KeyClass key_class_from_key_type(KeyType key_type) {
    return get_key_data(key_type).key_class_;
}

bool is_string_key_type(KeyType key_type){
    return variant_type_from_key_type(key_type) == VariantType::STRING_TYPE;
}

bool is_ref_key_class(KeyType key_type){
    return key_class_from_key_type(key_type) == KeyClass::REF_KEY;
}

}