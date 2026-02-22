/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstring>
#include <memory>
#include <string>

#include <sparrow/c_interface.hpp>

#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_output_options.hpp>

namespace arcticdb::bindings {

// Arrow C Stream Interface struct, per https://arrow.apache.org/docs/format/CStreamInterface.html
// Sparrow defines ArrowArray and ArrowSchema but not ArrowArrayStream.
struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

// Private data held by the ArrowArrayStream, wrapping a LazyRecordBatchIterator.
struct StreamPrivateData {
    std::shared_ptr<LazyRecordBatchIterator> iterator;
    StreamDescriptor descriptor;
    std::string last_error;
};

// ArrowArrayStream callback: export schema from the iterator's descriptor.
// Creates a zero-row RecordBatchData to extract the schema, matching the pattern
// used by the Python layer for empty results.
inline int stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
    auto* priv = static_cast<StreamPrivateData*>(stream->private_data);
    try {
        ArrowOutputConfig config;
        auto empty_batch = empty_record_batch_from_descriptor(priv->descriptor, config, std::nullopt);
        // Transfer schema ownership to the caller
        *out = empty_batch.schema_;
        // Prevent RecordBatchData destructor from releasing the schema we just transferred
        empty_batch.schema_.release = nullptr;
        return 0;
    } catch (const std::exception& e) {
        priv->last_error = e.what();
        return -1;
    }
}

// ArrowArrayStream callback: get next record batch from the iterator.
// Returns 0 on success. When exhausted, sets out->release = NULL per spec.
inline int stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
    auto* priv = static_cast<StreamPrivateData*>(stream->private_data);
    try {
        auto batch = priv->iterator->next();
        if (!batch.has_value()) {
            // End of stream: signal with release == NULL
            std::memset(out, 0, sizeof(ArrowArray));
            out->release = nullptr;
            return 0;
        }
        // Transfer array ownership to the caller
        *out = batch->array_;
        // Prevent RecordBatchData destructor from releasing what we transferred
        batch->array_.release = nullptr;
        // The schema is not transferred here (get_schema provides it once),
        // but we still need to clean up the per-batch schema
        return 0;
    } catch (const std::exception& e) {
        priv->last_error = e.what();
        return -1;
    }
}

// ArrowArrayStream callback: return last error message.
inline const char* stream_get_last_error(ArrowArrayStream* stream) {
    auto* priv = static_cast<StreamPrivateData*>(stream->private_data);
    return priv->last_error.c_str();
}

// ArrowArrayStream callback: release the stream and all owned resources.
inline void stream_release(ArrowArrayStream* stream) {
    if (stream->private_data) {
        delete static_cast<StreamPrivateData*>(stream->private_data);
        stream->private_data = nullptr;
    }
    stream->release = nullptr;
}

// Wrap a LazyRecordBatchIterator into an ArrowArrayStream.
// The caller must have allocated the ArrowArrayStream struct; this function fills it.
// Ownership of the iterator is transferred to the stream.
inline void wrap_iterator_as_arrow_stream(
        std::shared_ptr<LazyRecordBatchIterator> iterator, const StreamDescriptor& descriptor,
        ArrowArrayStream* out_stream
) {
    auto* priv = new StreamPrivateData{std::move(iterator), descriptor.clone(), {}};
    out_stream->get_schema = stream_get_schema;
    out_stream->get_next = stream_get_next;
    out_stream->get_last_error = stream_get_last_error;
    out_stream->release = stream_release;
    out_stream->private_data = priv;
}

} // namespace arcticdb::bindings
