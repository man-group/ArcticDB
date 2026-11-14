/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/async/tasks.hpp>
#include <arcticdb/codec/block_hash_check.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <algorithm>
#include <cstring>
#include <random>
#include <regex>

using namespace arcticdb;

namespace {

constexpr size_t num_rows = 1000;
constexpr const char* verify_option = "Codec.VerifyBlockHash";

// Random values, so LZ4 stores the "payload" column as literals and a value's bytes can be found in the encoded buffer.
std::vector<uint64_t> payload_values() {
    std::mt19937_64 gen{42};
    std::vector<uint64_t> values(num_rows);
    std::ranges::generate(values, gen);
    return values;
}

Segment encode_test_segment(EncodingVersion encoding_version) {
    const auto desc = stream_descriptor(
            StreamId{"sym"},
            stream::RowCountIndex{},
            {scalar_field(DataType::UINT64, "row"), scalar_field(DataType::UINT64, "payload")}
    );
    SegmentInMemory segment{desc.clone()};
    const auto payload = payload_values();
    for (size_t row = 0; row < num_rows; ++row) {
        segment.set_scalar(0, uint64_t(row));
        segment.set_scalar(1, payload[row]);
        segment.end_row();
    }
    return encode_dispatch(std::move(segment), codec::default_lz4_codec(), encoding_version);
}

uint8_t* find_payload_value(Segment& segment, size_t row) {
    auto* begin = const_cast<uint8_t*>(segment.buffer().data());
    auto* end = begin + segment.buffer().bytes();
    const auto value = payload_values()[row];
    const auto* value_bytes = reinterpret_cast<const uint8_t*>(&value);
    auto* found = std::search(begin, end, value_bytes, value_bytes + sizeof(value));
    util::check(found != end, "Payload value of row {} not found in the encoded segment", row);
    return found;
}

// Changes one byte of a value that LZ4 stored as a literal: LZ4 still decodes, to the wrong value.
void flip_payload_literal(Segment& segment) { *find_payload_value(segment, num_rows / 2) ^= 0x01; }

// The byte before the first literal is the last byte of the literal length; 0xFF makes the length run past the block.
void break_payload_lz4_sequence(Segment& segment) { *(find_payload_value(segment, 0) - 1) = 0xFF; }

const AtomKey test_key =
        atom_key_builder().version_id(7).start_index(0).end_index(num_rows).build(StreamId{"sym"}, KeyType::TABLE_DATA);

auto decode_with_key(Segment&& segment) {
    return async::DecodeSegmentTask{}(storage::KeySegmentPair{VariantKey{test_key}, std::move(segment)});
}

template<typename Exception>
std::string decode_error_message(Segment&& segment) {
    try {
        decode_with_key(std::move(segment));
    } catch (const Exception& e) {
        return e.what();
    }
    ADD_FAILURE() << "Decoding did not raise the expected exception";
    return {};
}

// The column buffer is chunked, so "payload" spans several blocks; it is field 1 (a RowCountIndex has no index field)
void expect_location_in_message(const std::string& message) {
    EXPECT_TRUE(
            std::regex_search(message, std::regex{R"(, in block \d+ of \d+, in column 'payload' \(field 1\), in key )"})
    ) << message;
    EXPECT_NE(message.find(fmt::format("in key {}", test_key)), std::string::npos) << message;
    EXPECT_NE(message.find("symbol 'sym', version 7"), std::string::npos) << message;
    EXPECT_NE(message.find(to_tokenized_key(test_key)), std::string::npos) << message;
    EXPECT_NE(message.find("If re-reading succeeds, the damage happened in transit"), std::string::npos) << message;
}

} // namespace

class BlockHashCheckTest : public testing::TestWithParam<EncodingVersion> {};

TEST_P(BlockHashCheckTest, IntactSegmentDecodesWithVerificationOn) {
    ScopedConfig verify(verify_option, 1);
    auto [key, decoded] = decode_with_key(encode_test_segment(GetParam()));
    ASSERT_EQ(decoded.row_count(), num_rows);
    EXPECT_EQ(decoded.scalar_at<uint64_t>(num_rows / 2, 1), payload_values()[num_rows / 2]);
}

TEST_P(BlockHashCheckTest, CorruptedLiteralRaisesDataIntegrityExceptionWithVerificationOn) {
    ScopedConfig verify(verify_option, 1);
    auto segment = encode_test_segment(GetParam());
    flip_payload_literal(segment);
    const auto message = decode_error_message<DataIntegrityException>(std::move(segment));
    EXPECT_TRUE(message.starts_with("E_DATA_INTEGRITY Block hash mismatch: stored 0x")) << message;
    EXPECT_NE(message.find("(LZ4 codec, "), std::string::npos) << message;
    EXPECT_NE(message.find(" decoded bytes), in block "), std::string::npos) << message;
    expect_location_in_message(message);
}

TEST_P(BlockHashCheckTest, CorruptedLiteralDecodesToWrongValueWithVerificationOff) {
    ScopedConfig verify = ScopedConfig::unset_int(verify_option);
    auto segment = encode_test_segment(GetParam());
    flip_payload_literal(segment);
    auto [key, decoded] = decode_with_key(std::move(segment));
    EXPECT_NE(decoded.scalar_at<uint64_t>(num_rows / 2, 1), payload_values()[num_rows / 2]);
}

TEST_P(BlockHashCheckTest, BrokenLz4SequenceNamesItsLocationWithVerificationOff) {
    ScopedConfig verify = ScopedConfig::unset_int(verify_option);
    auto segment = encode_test_segment(GetParam());
    break_payload_lz4_sequence(segment);
    const auto message = decode_error_message<CodecException>(std::move(segment));
    EXPECT_TRUE(message.starts_with("E_DECODE_ERROR Error while decoding with lz4 (code ")) << message;
    EXPECT_NE(message.find(" decoded bytes, in block 0 of "), std::string::npos) << message;
    expect_location_in_message(message);
}

TEST_P(BlockHashCheckTest, IntactSegmentsDecodeWithVerificationOnForEveryCodec) {
    ScopedConfig verify(verify_option, 1);
    arcticdb::proto::encoding::VariantCodec zstd;
    zstd.mutable_zstd()->set_level(0);
    const auto payload = payload_values();
    for (const auto& codec : {codec::default_lz4_codec(), zstd, codec::default_passthrough_codec()}) {
        SegmentInMemory segment{stream_descriptor(
                StreamId{"sym"},
                stream::RowCountIndex{},
                {scalar_field(DataType::UINT64, "payload"), scalar_field(DataType::UTF_DYNAMIC64, "text")}
        )};
        for (size_t row = 0; row < num_rows; ++row) {
            segment.set_scalar(0, payload[row]);
            segment.set_string(1, fmt::format("s{}", row % 7));
            segment.end_row();
        }
        auto encoded = encode_dispatch(std::move(segment), codec, GetParam());
        auto decoded = decode_segment(encoded);
        ASSERT_EQ(decoded.row_count(), num_rows) << codec.DebugString();
        EXPECT_EQ(decoded.string_at(num_rows - 1, 1), fmt::format("s{}", (num_rows - 1) % 7)) << codec.DebugString();
    }
}

TEST_P(BlockHashCheckTest, SettingIsReadAtEachSegmentDecode) {
    ScopedConfig restore = ScopedConfig::unset_int(verify_option);
    auto corrupted_segment = [] {
        auto segment = encode_test_segment(GetParam());
        flip_payload_literal(segment);
        return segment;
    };
    EXPECT_NO_THROW(decode_with_key(corrupted_segment()));
    ConfigsMap::instance()->set_int(verify_option, 1);
    EXPECT_THROW(decode_with_key(corrupted_segment()), DataIntegrityException);
    ConfigsMap::instance()->set_int(verify_option, 0);
    EXPECT_NO_THROW(decode_with_key(corrupted_segment()));
    {
        ScopedConfig on(verify_option, 1);
        EXPECT_THROW(decode_with_key(corrupted_segment()), DataIntegrityException);
    }
    EXPECT_NO_THROW(decode_with_key(corrupted_segment()));
}

INSTANTIATE_TEST_SUITE_P(
        EncodingVersions, BlockHashCheckTest, testing::Values(EncodingVersion::V1, EncodingVersion::V2),
        [](const auto& info) { return info.param == EncodingVersion::V1 ? "V1" : "V2"; }
);
