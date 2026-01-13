/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/magic_num.hpp>

namespace arcticdb {
using DescriptorFieldsMagic = util::MagicNum<'D', 'e', 's', 'c'>;
using EncodedMagic = util::MagicNum<'E', 'n', 'c', 'd'>;
using StringPoolMagic = util::MagicNum<'S', 't', 'r', 'p'>;
using MetadataMagic = util::MagicNum<'M', 'e', 't', 'a'>;
using IndexMagic = util::MagicNum<'I', 'n', 'd', 'x'>;
using ColumnMagic = util::MagicNum<'C', 'l', 'm', 'n'>;
using FrameMetadataMagic = util::MagicNum<'F', 'r', 'a', 'm'>;
using SegmentDescriptorMagic = util::MagicNum<'S', 'D', 's', 'c'>;
} // namespace arcticdb
