// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/**
 * @file
 * @brief Universally unique identifier.
 */

#pragma once

#include "azure/core/platform.hpp"

#include <array>
#include <cstdint> // defines std::uint8_t
#include <cstring>
#include <stdint.h> // deprecated, defines uint8_t in global namespace. TODO: Remove in the future when references to uint8_t and friends are removed.
#include <string>

namespace Azure { namespace Core {
  /**
   * @brief Universally unique identifier.
   */
  class Uuid final {

  private:
    static constexpr size_t UuidSize = 16;

    std::array<uint8_t, UuidSize> m_uuid{};

  private:
    Uuid(uint8_t const uuid[UuidSize]) { std::memcpy(m_uuid.data(), uuid, UuidSize); }

  public:
    /**
     * @brief Gets Uuid as a string.
     * @details A string is in canonical format (4-2-2-2-6 lowercase hex and dashes only).
     */
    std::string ToString();

    /**
     * @brief Returns the binary value of the Uuid for consumption by clients who need non-string
     * representation of the Uuid.
     * @returns An array with the binary representation of the Uuid.
     */
    std::array<uint8_t, UuidSize> const& AsArray() const { return m_uuid; }

    /**
     * @brief Creates a new random UUID.
     *
     */
    static Uuid CreateUuid();

    /**
     * @brief Construct a Uuid from an existing UUID represented as an array of bytes.
     * @details Creates a Uuid from a UUID created in an external scope.
     */
    static Uuid CreateFromArray(std::array<uint8_t, UuidSize> const& uuid);
  };
}} // namespace Azure::Core
