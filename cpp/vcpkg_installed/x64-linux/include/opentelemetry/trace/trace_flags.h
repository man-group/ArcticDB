// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

#include "opentelemetry/nostd/span.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace trace
{

// TraceFlags represents options for a Trace. These options are propagated to all child Spans
// and determine features such as whether a Span should be traced. TraceFlags
// are implemented as a bitmask.
class TraceFlags final
{
public:
  static constexpr uint8_t kIsSampled = 1;

  TraceFlags() noexcept : rep_{0} {}

  explicit TraceFlags(uint8_t flags) noexcept : rep_(flags) {}

  bool IsSampled() const noexcept { return rep_ & kIsSampled; }

  // Populates the buffer with the lowercase base16 representation of the flags.
  void ToLowerBase16(nostd::span<char, 2> buffer) const noexcept
  {
    constexpr char kHex[] = "0123456789ABCDEF";
    buffer[0]             = kHex[(rep_ >> 4) & 0xF];
    buffer[1]             = kHex[(rep_ >> 0) & 0xF];
  }

  uint8_t flags() const noexcept { return rep_; }

  bool operator==(const TraceFlags &that) const noexcept { return rep_ == that.rep_; }

  bool operator!=(const TraceFlags &that) const noexcept { return !(*this == that); }

  // Copies the TraceFlags to dest.
  void CopyBytesTo(nostd::span<uint8_t, 1> dest) const noexcept { dest[0] = rep_; }

private:
  uint8_t rep_;
};

}  // namespace trace
OPENTELEMETRY_END_NAMESPACE
