/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/decimal.hpp>
#include <string>
#include <cassert>
#include <cmath>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <boost/multiprecision/cpp_int.hpp>

namespace arcticdb {

namespace util {
// By word we refer to string of digits, not a CPU word
constexpr static int max_digits_per_word = std::numeric_limits<int64_t>::digits10;
constexpr static std::array<uint64_t, std::numeric_limits<uint64_t>::digits10 + 1> powers_of_10{
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000,
        100000000000,
        1000000000000,
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000ULL
};

constexpr static auto check_valid_decimal =
        arcticdb::util::detail::Check<ErrorCode::E_INVALID_DECIMAL_STRING, ErrorCategory::USER_INPUT>{};

[[nodiscard]] static inline bool is_digit(const char c) { return c >= '0' && c <= '9'; }

[[nodiscard]] static inline bool is_exponent_symbol(const char c) { return c == 'E' || c == 'e'; }

static inline void trim_leading_zeroes(std::string_view& str) {
    const size_t first_non_zero = str.find_first_not_of('0');
    if (first_non_zero != std::string::npos) {
        str.remove_prefix(first_non_zero);
    }
}

[[nodiscard]] static inline boost::multiprecision::uint128_t to_uint128(
        const uint64_t most_significant, const uint64_t least_significant
) {
    boost::multiprecision::uint128_t number = most_significant;
    number <<= 64;
    number |= least_significant;
    return number;
}

// Used instead of std::strtoull to parse chuncks of strings which can contain numbers larger than what ull
// can hold. std::strtoull would try to parse the whole underlying string (and potentially overflow) while this
// stops when the string_view length is reached.
[[nodiscard]] static inline uint64_t to_uint64_t(std::string_view str) {
    assert(str.size() < powers_of_10.size());
    uint64_t result = 0;
    for (int i = str.size() - 1; i >= 0; --i) {
        assert(is_digit(str[i]));
        const int power = str.length() - i - 1;
        result += (str[i] - '0') * powers_of_10[power];
    }
    return result;
}

/**
 * Take a decimal number with up to 38 significant digits and
 * extract the exponent, sign and all digits. At the end all
 * digits (including the zeros from multiplying by 10^exponent)
 * will be contained in an array, accessible by get_digits().
 * The array will contain digits only.
 */
class NumberComponents {
  public:
    explicit NumberComponents(std::string_view input);
    [[nodiscard]] bool is_negative() const;
    [[nodiscard]] bool is_decimal() const;
    [[nodiscard]] const char* get_digits() const;
    [[nodiscard]] int get_size() const;
    constexpr static int max_digits = 38;

  private:
    struct SpecialSymbols {
        int decimal_point_position = -1;
        int exponent_position = -1;
    };

    enum NumberComponentsFlags { NEGATIVE = 1, DECIMAL = 1 << 1 };

    void handle_sign(std::string_view& input);
    void handle_exponent(std::string_view& input, int exponent_position);
    SpecialSymbols scan_for_special_symbols(std::string_view input);
    void expand_exponent(int decimal_point_position);
    void parse_digits(std::string_view input);

    std::array<char, max_digits + 1> digits_;
    int exponent_;
    int size_;
    unsigned flags_;
};

NumberComponents::NumberComponents(std::string_view input) : digits_{'\0'}, exponent_(0), size_(0), flags_(0) {

    handle_sign(input);
    trim_leading_zeroes(input);
    const SpecialSymbols special_symbols = scan_for_special_symbols(input);
    handle_exponent(input, special_symbols.exponent_position);
    parse_digits(input);
    expand_exponent(special_symbols.decimal_point_position);
}

void NumberComponents::handle_sign(std::string_view& input) {
    if (input[0] == '-') {
        flags_ |= NumberComponentsFlags::NEGATIVE;
        input.remove_prefix(1);
    }
}

void NumberComponents::handle_exponent(std::string_view& input, int exponent_position) {
    if (exponent_position != -1) {
        size_t processed_digits_count;
        exponent_ = std::stoi(input.data() + exponent_position + 1, &processed_digits_count);
        check_valid_decimal(
                exponent_position + processed_digits_count == input.length() - 1,
                "Cannot parse decimal from string. Cannot parse exponent."
        );
        input = input.substr(0, exponent_position);
    }
}

NumberComponents::SpecialSymbols NumberComponents::scan_for_special_symbols(std::string_view input) {
    SpecialSymbols result;
    for (size_t i = 0; i < input.length(); ++i) {
        const char current_symbol = input[i];
        if (current_symbol == '.') {
            check_valid_decimal(
                    !is_decimal(),
                    "Cannot parse decimal from string. "
                    "Invalid character '{}'. More than one decimal points are not allowed.",
                    current_symbol
            );
            flags_ |= NumberComponentsFlags::DECIMAL;
            result.decimal_point_position = i;
        } else if (is_exponent_symbol(current_symbol)) {
            result.exponent_position = i;
            break;
        } else {
            check_valid_decimal(
                    is_digit(current_symbol),
                    "Cannot parse decimal from string. Invalid character '{}'.",
                    current_symbol
            );
        }
    }
    return result;
}

void NumberComponents::parse_digits(std::string_view input) {
    for (const char symbol : input) {
        check_valid_decimal(
                size_ < max_digits,
                "Cannot parse decimal from string. "
                "Overflow. Input has more than {} significant digits.",
                max_digits
        );
        if (ARCTICDB_LIKELY(is_digit(symbol))) {
            digits_[size_++] = symbol;
        } else if (ARCTICDB_UNLIKELY(symbol == '.')) {
            continue;
        }
    }
}

void NumberComponents::expand_exponent(int decimal_point_position) {
    if (size_ == 0) {
        digits_[size_++] = '0';
    }

    const int digits_after_decimal_point = is_decimal() ? size_ - decimal_point_position : 0;
    const int zeros_to_append = std::max(0, exponent_ - digits_after_decimal_point);
    check_valid_decimal(
            size_ + zeros_to_append <= max_digits,
            "Cannot parse decimal from string. "
            "Overflow. Input has more than {} significant digits.",
            max_digits
    );
    for (int i = 0; i < zeros_to_append; ++i) {
        digits_[size_++] = '0';
    }
}

bool NumberComponents::is_negative() const { return flags_ & NumberComponentsFlags::NEGATIVE; }

bool NumberComponents::is_decimal() const { return flags_ & NumberComponentsFlags::DECIMAL; }

const char* NumberComponents::get_digits() const { return digits_.data(); }

int NumberComponents::get_size() const { return size_; }

Decimal::Decimal() : data_{0} {}

Decimal::Decimal(std::string_view number) : data_{0} {
    // GCC and Clang have internal 2's complement __uint128_t.
    // MSVC does not have 128-bit integer, it has __m128, which is for SIMD.
    // Boost's uint128_t is not in two's complement, so it cannot be reinterpret_cast over the data.
    // TODO: potential optimization for Clang/GCC would be to load it __uint128_t and reinterpret_cast
    //  it over the array.
    const NumberComponents components(number);
    std::string_view number_to_parse(components.get_digits());
    while (!number_to_parse.empty()) {
        const std::string_view chunk = number_to_parse.substr(0, max_digits_per_word);
        push_chunk(chunk);
        number_to_parse = number_to_parse.substr(chunk.size());
    }
    if (components.is_negative()) {
        negate();
    }
}

void Decimal::push_chunk(std::string_view chunk_str) {
    uint64_t chunk = to_uint64_t(chunk_str);
    const uint64_t word_exponent = powers_of_10[chunk_str.length()];
    const uint64_t mask = 0xFFFFFFFFFFFFFFFFULL;
    for (uint64_t& word : data_) {
        boost::multiprecision::uint128_t tmp = word;
        tmp *= word_exponent;
        tmp += chunk;
        word = static_cast<uint64_t>(tmp & mask);
        chunk = static_cast<uint64_t>(tmp >> 64);
    }
}

std::string Decimal::to_string(int scale) const {
    if (is_negative()) {
        const Decimal negated = this->operator-();
        std::string result = negated.to_string(scale);
        result.insert(0, "-");
        return result;
    }
    if (data_[0] == 0 && data_[1] == 0) {
        return "0";
    }

    boost::multiprecision::uint128_t number =
            to_uint128(data_[MOST_SIGNIFICANT_WORD_INDEX], data_[LEAST_SIGNIFICANT_WORD_INDEX]);

    std::string result;
    if (scale < 0) {
        std::fill_n(std::back_inserter(result), -scale, '0');
        scale = 0;
    }
    while (number) {
        const char digit = static_cast<char>(number % 10) + '0';
        result.push_back(digit);
        number /= 10;
    }
    if (scale > 0) {
        const int len = static_cast<int>(result.length());
        if (len > scale) {
            result.insert(scale, ".");
        } else {
            std::fill_n(std::back_inserter(result), std::abs(len - scale), '0');
            result.append(".0");
        }
    }
    std::reverse(result.begin(), result.end());
    return result;
}

bool Decimal::is_negative() const { return static_cast<int64_t>(data_[MOST_SIGNIFICANT_WORD_INDEX]) < 0; }

void Decimal::negate() {
    data_[LEAST_SIGNIFICANT_WORD_INDEX] = ~data_[LEAST_SIGNIFICANT_WORD_INDEX];
    data_[LEAST_SIGNIFICANT_WORD_INDEX] += 1;

    data_[MOST_SIGNIFICANT_WORD_INDEX] = ~data_[MOST_SIGNIFICANT_WORD_INDEX];
    if (data_[LEAST_SIGNIFICANT_WORD_INDEX] == 0) {
        data_[LEAST_SIGNIFICANT_WORD_INDEX]++;
    }
}

Decimal Decimal::operator-() const {
    Decimal result(*this);
    result.negate();
    return result;
}

bool Decimal::operator==(const arcticdb::util::Decimal& other) const { return data_ == other.data_; }
} // namespace util
} // namespace arcticdb