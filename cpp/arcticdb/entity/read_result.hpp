/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/pipeline/python_output_frame.hpp>
#include <arcticdb/util/memory_tracing.hpp>

#include <vector>

namespace arcticdb {

struct ARCTICDB_VISIBILITY_HIDDEN ReadResult {
  ReadResult(const VersionedItem& versioned_item,
             pipelines::PythonOutputFrame&& frame_data,
             const arcticdb::proto::descriptors::NormalizationMetadata& norm_meta,
             const arcticdb::proto::descriptors::UserDefinedMetadata& user_meta,
             const arcticdb::proto::descriptors::UserDefinedMetadata& multi_key_meta,
             std::vector<entity::AtomKey>&& multi_keys)
      : item(versioned_item), frame_data(std::move(frame_data)), norm_meta(norm_meta),
        user_meta(user_meta), multi_key_meta(multi_key_meta),
        multi_keys(std::move(multi_keys)) {}
  VersionedItem item;
  pipelines::PythonOutputFrame frame_data;
  arcticdb::proto::descriptors::NormalizationMetadata norm_meta;
  arcticdb::proto::descriptors::UserDefinedMetadata user_meta;
  arcticdb::proto::descriptors::UserDefinedMetadata multi_key_meta;
  std::vector<entity::AtomKey> multi_keys;

  ARCTICDB_MOVE_ONLY_DEFAULT(ReadResult)
};

inline ReadResult create_python_read_result(const VersionedItem& version,
                                            FrameAndDescriptor&& fd) {
  auto result = std::move(fd);
  auto python_frame = pipelines::PythonOutputFrame{result.frame_, result.buffers_};
  util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);

  const auto& desc_proto = result.desc_.proto();
  return {version,
          std::move(python_frame),
          desc_proto.normalization(),
          desc_proto.user_meta(),
          desc_proto.multi_key_meta(),
          std::move(result.keys_)};
}
} // namespace arcticdb