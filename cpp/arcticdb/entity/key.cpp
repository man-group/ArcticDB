/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/entity/key.hpp>

namespace arcticdb::entity {

#define NUMERIC_KEY(kt, name, c) template<> const char* KeyData<kt>::long_name_ = #name;  \
    template<> const char KeyData<kt>::short_name_ = c; \
    template<> const VariantType KeyData<kt>::variant_type_ = VariantType::NUMERIC_TYPE; \
    template<>  const KeyClass KeyData<kt>::key_class_ = KeyClass::ATOM_KEY; \
    template<> const char* KeyData<kt>::description_ = #kt;

#define STRING_KEY(kt, name, c) template<> const char* KeyData<kt>::long_name_ = #name; \
    template<> const char KeyData<kt>::short_name_ = c; \
    template<> const VariantType KeyData<kt>::variant_type_ = VariantType::STRING_TYPE; \
    template<> const KeyClass KeyData<kt>::key_class_ = KeyClass::ATOM_KEY; \
    template<> const char* KeyData<kt>::description_ = #kt;

#define STRING_REF(kt, name, c) template<> const char* KeyData<kt>::long_name_ = #name; \
    template<> const char KeyData<kt>::short_name_ = c; \
    template<> const VariantType KeyData<kt>::variant_type_ = VariantType::STRING_TYPE; \
    template<> const KeyClass KeyData<kt>::key_class_ = KeyClass::REF_KEY; \
    template<> const char* KeyData<kt>::description_ = #kt;


NUMERIC_KEY(KeyTagType<KeyType::STREAM_GROUP>, sg, 'g')
NUMERIC_KEY(KeyTagType<KeyType::GENERATION>, gen, 'G')
STRING_KEY(KeyTagType<KeyType::TABLE_DATA>, tdata, 'd')
STRING_KEY(KeyTagType<KeyType::TABLE_INDEX>, tindex, 'i')
STRING_KEY(KeyTagType<KeyType::VERSION>, ver, 'V')
STRING_KEY(KeyTagType<KeyType::VERSION_JOURNAL>, vj, 'v')
STRING_KEY(KeyTagType<KeyType::METRICS>, met, 'M')
STRING_KEY(KeyTagType<KeyType::SNAPSHOT>, snap, 's')
STRING_KEY(KeyTagType<KeyType::SYMBOL_LIST>, sl, 'l')
STRING_REF(KeyTagType<KeyType::VERSION_REF>, vref, 'r')
STRING_REF(KeyTagType<KeyType::STORAGE_INFO>, sref, 'h')
STRING_REF(KeyTagType<KeyType::APPEND_REF>, aref, 'a')
STRING_KEY(KeyTagType<KeyType::MULTI_KEY>, mref, 'm')
STRING_REF(KeyTagType<KeyType::LOCK>, lref, 'x')
STRING_REF(KeyTagType<KeyType::SNAPSHOT_REF>, tref, 't')
STRING_REF(KeyTagType<KeyType::SNAPSHOT_TOMBSTONE>, ttomb, 'X')
STRING_KEY(KeyTagType<KeyType::PARTITION>, pref, 'p')
STRING_KEY(KeyTagType<KeyType::TOMBSTONE>, tomb, 'x')
STRING_KEY(KeyTagType<KeyType::APPEND_DATA>, app, 'b')
STRING_KEY(KeyTagType<KeyType::LOG>, log, 'o')
STRING_KEY(KeyTagType<KeyType::LOG_COMPACTED>, logc, 'O')
STRING_REF(KeyTagType<KeyType::OFFSET>, off, 'f')
STRING_REF(KeyTagType<KeyType::BACKUP_SNAPSHOT_REF>, bref, 'B')
STRING_KEY(KeyTagType<KeyType::TOMBSTONE_ALL>, tall, 'q')
STRING_REF(KeyTagType<KeyType::LIBRARY_CONFIG>, cref, 'C')
STRING_KEY(KeyTagType<KeyType::COLUMN_STATS>, cstats, 'S')

}