/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/bindings/arcticdb_c.h>
#include <arcticdb/bindings/arrow_stream.hpp>

#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/types.hpp>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <string>

namespace {

void set_error(ArcticError* err, int code, const char* msg) {
    if (!err)
        return;
    err->code = code;
    std::strncpy(err->message, msg, sizeof(err->message) - 1);
    err->message[sizeof(err->message) - 1] = '\0';
}

void clear_error(ArcticError* err) {
    if (err) {
        err->code = 0;
        err->message[0] = '\0';
    }
}

} // anonymous namespace

// The opaque handle exposed through the C API.
struct ArcticLibrary {
    std::shared_ptr<arcticdb::storage::Library> library;
    std::unique_ptr<arcticdb::version_store::LocalVersionedEngine> engine;
};

extern "C" {

int arctic_library_open_lmdb(const char* path, ArcticLibrary** out, ArcticError* err) {
    clear_error(err);
    if (!path || !out) {
        set_error(err, 1, "NULL argument");
        return 1;
    }
    try {
        namespace storage = arcticdb::storage;

        std::filesystem::create_directories(path);

        auto library_path = storage::LibraryPath::from_delim_path("arcticdb_c.default");
        auto lmdb_config = storage::lmdb::pack_config(path);

        // Build the Library with a VersionStoreConfig for proper engine initialization
        arcticdb::proto::storage::VersionStoreConfig vs_config;
        vs_config.set_symbol_list(true);

        auto library = std::make_shared<storage::Library>(
                library_path,
                storage::create_storages(library_path, storage::OpenMode::DELETE, {lmdb_config}),
                vs_config
        );

        auto engine = std::make_unique<arcticdb::version_store::LocalVersionedEngine>(library);

        auto* handle = new ArcticLibrary{std::move(library), std::move(engine)};
        *out = handle;
        return 0;
    } catch (const std::exception& e) {
        set_error(err, 2, e.what());
        return 2;
    }
}

void arctic_library_close(ArcticLibrary* lib) { delete lib; }

int arctic_write_test_data(
        ArcticLibrary* lib, const char* symbol, int64_t num_rows, int64_t num_columns, ArcticError* err
) {
    clear_error(err);
    if (!lib || !symbol) {
        set_error(err, 1, "NULL argument");
        return 1;
    }
    if (num_rows <= 0 || num_columns <= 0) {
        set_error(err, 1, "num_rows and num_columns must be positive");
        return 1;
    }
    try {
        using namespace arcticdb;
        using namespace arcticdb::entity;

        // Build field descriptors: one float64 column per requested column
        std::vector<FieldRef> fields;
        std::vector<std::string> col_names;
        col_names.reserve(static_cast<size_t>(num_columns));
        for (int64_t c = 0; c < num_columns; ++c) {
            col_names.push_back(fmt::format("col_{}", c));
        }
        for (int64_t c = 0; c < num_columns; ++c) {
            fields.push_back(scalar_field(DataType::FLOAT64, col_names[static_cast<size_t>(c)]));
        }

        auto desc = stream::TimeseriesIndex::default_index().create_stream_descriptor(
                StreamId{std::string(symbol)}, std::ranges::subrange(fields.begin(), fields.end())
        );

        auto rows = static_cast<size_t>(num_rows);
        SegmentInMemory seg(std::move(desc), rows);

        // Fill index column (column 0)
        auto& idx_col = seg.column(0);
        for (size_t i = 0; i < rows; ++i) {
            idx_col.set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        }

        // Fill data columns (columns 1..num_columns)
        for (int64_t c = 0; c < num_columns; ++c) {
            auto& data_col = seg.column(static_cast<position_t>(c + 1));
            for (size_t i = 0; i < rows; ++i) {
                data_col.set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5 * (c + 1));
            }
        }
        seg.set_row_data(rows - 1);

        lib->engine->write_segment(
                StreamId{std::string(symbol)}, std::move(seg), false, version_store::Slicing::RowSlicing
        );
        return 0;
    } catch (const std::exception& e) {
        set_error(err, 2, e.what());
        return 2;
    }
}

int arctic_read_stream(
        ArcticLibrary* lib, const char* symbol, int64_t version, struct ArcticArrowArrayStream* out, ArcticError* err
) {
    clear_error(err);
    if (!lib || !symbol || !out) {
        set_error(err, 1, "NULL argument");
        return 1;
    }
    try {
        using namespace arcticdb;
        using namespace arcticdb::pipelines;

        StreamId stream_id{std::string(symbol)};

        // Resolve version
        VersionQuery version_query;
        if (version >= 0) {
            version_query.set_version(static_cast<SignedVersionId>(version), false);
        }
        // else: default (monostate) = latest

        auto opt_version = lib->engine->get_version_to_read(stream_id, version_query);
        if (!opt_version) {
            set_error(err, 3, "Symbol or version not found");
            return 3;
        }

        // Set up pipeline context (reads index, builds SliceAndKey vector)
        ReadQuery read_query;
        ReadOptions read_options;
        read_options.set_output_format(OutputFormat::ARROW);

        auto pipeline_context = version_store::setup_pipeline_context(
                lib->engine->_test_get_store(), *opt_version, read_query, read_options
        );

        // Sort slice_and_keys by (row_range, col_range) for column-slice merging
        std::sort(
                pipeline_context->slice_and_keys_.begin(),
                pipeline_context->slice_and_keys_.end(),
                [](const auto& a, const auto& b) {
                    return std::tie(a.slice_.row_range.first, a.slice_.col_range.first) <
                           std::tie(b.slice_.row_range.first, b.slice_.col_range.first);
                }
        );

        // Populate overall_column_bitset_ for column pushdown
        get_column_bitset_in_context(read_query, pipeline_context);

        // Build columns_to_decode
        std::shared_ptr<std::unordered_set<std::string>> cols_to_decode;
        if (pipeline_context->overall_column_bitset_) {
            cols_to_decode = std::make_shared<std::unordered_set<std::string>>();
            auto en = pipeline_context->overall_column_bitset_->first();
            auto en_end = pipeline_context->overall_column_bitset_->end();
            while (en < en_end) {
                cols_to_decode->insert(std::string(pipeline_context->desc_->field(*en++).name()));
            }
        }

        // Create LazyRecordBatchIterator
        auto iterator = std::make_shared<LazyRecordBatchIterator>(
                std::move(pipeline_context->slice_and_keys_),
                pipeline_context->descriptor(),
                lib->engine->_test_get_store(),
                std::move(cols_to_decode),
                read_query.row_filter, // no filter
                nullptr,               // no expression context
                std::string{},         // no filter root node
                std::max(size_t{2}, pipeline_context->slice_and_keys_.size()),
                4ULL * 1024 * 1024 * 1024,
                read_options
        );

        // Wrap in ArrowArrayStream
        // The C header uses ArcticArrowArrayStream which has identical layout to bindings::ArrowArrayStream
        static_assert(sizeof(ArcticArrowArrayStream) == sizeof(bindings::ArrowArrayStream));
        bindings::wrap_iterator_as_arrow_stream(
                std::move(iterator), pipeline_context->descriptor(), reinterpret_cast<bindings::ArrowArrayStream*>(out)
        );
        return 0;
    } catch (const std::exception& e) {
        set_error(err, 2, e.what());
        return 2;
    }
}

int arctic_list_symbols(ArcticLibrary* lib, char*** out_symbols, int64_t* out_count, ArcticError* err) {
    clear_error(err);
    if (!lib || !out_symbols || !out_count) {
        set_error(err, 1, "NULL argument");
        return 1;
    }
    try {
        auto symbols = lib->engine->list_streams_internal(
                std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt
        );

        auto count = static_cast<int64_t>(symbols.size());
        auto** arr = static_cast<char**>(std::malloc(static_cast<size_t>(count) * sizeof(char*)));
        if (!arr && count > 0) {
            set_error(err, 4, "malloc failed");
            return 4;
        }

        int64_t idx = 0;
        for (const auto& sym : symbols) {
            auto sym_str = fmt::format("{}", sym);
            arr[idx] = static_cast<char*>(std::malloc(sym_str.size() + 1));
            std::strcpy(arr[idx], sym_str.c_str());
            ++idx;
        }

        *out_symbols = arr;
        *out_count = count;
        return 0;
    } catch (const std::exception& e) {
        set_error(err, 2, e.what());
        return 2;
    }
}

void arctic_free_symbols(char** symbols, int64_t count) {
    if (!symbols)
        return;
    for (int64_t i = 0; i < count; ++i) {
        std::free(symbols[i]);
    }
    std::free(symbols);
}

} // extern "C"
