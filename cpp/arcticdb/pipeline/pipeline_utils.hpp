/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>

namespace arcticdb::pipelines {

inline ReadResult make_read_result_from_frame(FrameAndDescriptor& frame_and_desc,
                                              const AtomKey& key) {
  auto pipeline_context =
      std::make_shared<PipelineContext>(frame_and_desc.frame_.descriptor());
  SliceAndKey sk{FrameSlice{frame_and_desc.frame_}, key};
  pipeline_context->slice_and_keys_.emplace_back(std::move(sk));
  util::BitSet bitset(1);
  bitset.flip();
  pipeline_context->fetch_index_ = std::move(bitset);
  pipeline_context->ensure_vectors();

  generate_filtered_field_descriptors(pipeline_context, {});
  pipeline_context->begin()->set_string_pool(frame_and_desc.frame_.string_pool_ptr());
  auto descriptor =
      std::make_shared<StreamDescriptor>(frame_and_desc.frame_.descriptor());
  pipeline_context->begin()->set_descriptor(std::move(descriptor));
  reduce_and_fix_columns(pipeline_context, frame_and_desc.frame_, ReadOptions{});

  return create_python_read_result(VersionedItem{key}, std::move(frame_and_desc));
}

} // namespace arcticdb::pipelines