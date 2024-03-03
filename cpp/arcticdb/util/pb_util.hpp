/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/format.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/any.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/util/message_differencer.h>
#include <folly/Range.h>

#include <exception>
#include <optional>

namespace arcticdb::util {

template<class Msg, class ExcType=std::invalid_argument>
[[noreturn]] void raise_error_msg(const char *pattern, const Msg &msg) {
   // google::protobuf::TextFormat::PrintToString(msg, &s);
    throw ExcType(fmt::format(fmt::runtime(pattern), msg.DebugString()));
}

namespace {
constexpr char TYPE_URL[] = "cxx.arctic.org";
}

template<class Msg>
void pack_to_any(const Msg &msg, google::protobuf::Any &any) {
    any.PackFrom(msg, TYPE_URL);
}

inline folly::StringPiece get_arcticdb_pb_type_name(const google::protobuf::Any &any) {
    folly::StringPiece sp{any.type_url()};
    if (!sp.startsWith(TYPE_URL)) {
        raise_error_msg("Not a valid arcticc proto msg", any);
    }
    return sp.subpiece(sizeof(TYPE_URL), sp.size());
}

template<class Msg>
bool pb_equals(const Msg &a, const Msg &b) {
    return google::protobuf::util::MessageDifferencer::Equals(a, b);
}

template<class T>
std::optional<T> as_opt(T val, const T &sentinel = T()) {
    if (val == sentinel) {
        return std::nullopt;
    }
    return std::make_optional(val);
}

inline std::string format(const google::protobuf::Message &msg) {
    std::string dest;
    google::protobuf::TextFormat::Printer p;
    p.SetExpandAny(true);
    p.PrintToString(msg, &dest);
    return dest;
}

inline std::string newlines_to_spaces(const ::google::protobuf::Message& msg) {
    auto out = msg.DebugString();
    std::replace(std::begin(out), std::end(out), '\n', ' ');
    return out;
}

} // namespace arctic::util

