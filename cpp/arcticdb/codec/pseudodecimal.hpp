#include <cmath>
#include <vector>
#include <cstddef>
#include <cstdint>

#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/configs_map.hpp>

// -------------------------------------------------------------------------------------
namespace arcticdb {

struct DecimalStructure {
    struct __attribute__((packed)) Slot {
        uint8_t d1 : 4;
        uint8_t d2 : 4;
    };
    static_assert(sizeof(Slot) == 1);
    // -------------------------------------------------------------------------------------
    uint32_t converted_count;
    // -------------------------------------------------------------------------------------
    uint8_t numbers_scheme;
    uint8_t exponents_scheme;
    uint8_t patches_scheme;
    uint8_t variant_selector;
    // -------------------------------------------------------------------------------------
    uint32_t exponents_offset;
    uint32_t patches_offset;
    uint32_t exceptions_map_offset;
    // -------------------------------------------------------------------------------------
    uint8_t data[];
};

constexpr uint32_t block_size = 4;

const uint32_t max_exponent = 22;
const uint8_t exponent_exception_code = 23;
const uint8_t decimal_index_mask = 0x1F;
static const double exact_fractions_of_ten[] = {
    1.0,
    0.1,
    0.01,
    0.001,
    0.0001,
    0.00001,
    0.000001,
    0.0000001,
    0.00000001,
    0.000000001,
    0.0000000001,
    0.00000000001,
    0.000000000001,
    0.0000000000001,
    0.00000000000001,
    0.000000000000001,
    0.0000000000000001,
    0.00000000000000001,
    0.000000000000000001,
    0.0000000000000000001,
    0.00000000000000000001,
    0.000000000000000000001,
    0.0000000000000000000001,
};
static_assert(sizeof(exact_fractions_of_ten) == sizeof(double) * 23);

// -------------------------------------------------------------------------------------
uint32_t compress(const double* src,
                      const util::BitMagic*,
                      uint8_t* dest,
                      DoubleStats& stats,
                      uint8_t allowed_cascading_level) {
    auto significant_digits_bits_limit = ConfigsMap::instance()->get_int("PseudoDecimal.SignificantDigitsLimit", 31);
    // Layout : Header | numbers_v | exponent_v | patches_v
    // ignore bitmap
    auto& col_struct = *reinterpret_cast<DecimalStructure*>(dest);
    col_struct.variant_selector = 0;
    std::vector<int32_t> numbers_v;
    std::vector<int32_t> exponent_v;
    std::vector<double> patches_v;  // patches

    uint32_t exception_count = 0;
    uint8_t run_count = 0;

    util::BitSet exceptions_bitmap;
    const uint32_t num_blocks = (stats.tuple_count + (block_size - 1)) / block_size;
    for (uint32_t block_i = 0; block_i < num_blocks; block_i++) {
        bool block_has_exception = false;

        const uint32_t row_start_i = block_i * block_size;
        const uint32_t row_end_i = std::min(row_start_i + block_size, stats.tuple_count);
        for (uint32_t row_i = row_start_i; row_i < row_end_i; row_i++) {
            double current_double = src[row_i];

            bool convertable = false;
            uint32_t exponent;
            uint64_t converted_number;
            if (current_double == -0.0 && std::signbit(current_double)) {
                // Special case -0.0 is handled as exception
                exponent = exponent_exception_code;
            } else {
                // Attempt conversion
                for (exponent = 0; exponent <= max_exponent; exponent++) {
                    double cd = current_double / exact_fractions_of_ten[exponent];
                    cd = std::round(cd);
                    converted_number = static_cast<uint64_t>(cd);
                    double if_converted_back =
                        static_cast<double>(converted_number) * exact_fractions_of_ten[exponent];
                    if (if_converted_back == current_double &&
                        ((std::floor(std::log2(converted_number)) + 1) <= significant_digits_bits_limit)) {
                        convertable = true;
                        break;
                    }
                }
            }

            // Write result
            if (convertable) {
                util::check((std::floor(std::log2(converted_number)) + 1) > 31, "log2 of {} too large", converted_number);
                exponent_v.push_back(static_cast<int32_t>(exponent));
                numbers_v.push_back(static_cast<int32_t>(converted_number));
            } else {
                block_has_exception = true;
                exception_count++;
                if (exception_count > stats.tuple_count / 2) {
                    // This is a hacky way to avoid using Decimal in columns where there
                    // are many exceptions Return a big number will make the selection
                    // process select uncompressed rather than Decimal
                    return stats.total_size + 1000;
                }
                exponent_v.push_back(exponent_exception_code);
                patches_v.push_back(src[row_i]);
            }
        }

        if (block_has_exception) {
            run_count = 0;
            exceptions_bitmap.add(block_i);
        } else {
            run_count++;
            col_struct.variant_selector |= do_iteration;
            if (run_count >= 4) {
                col_struct.variant_selector |= do_unroll;
            }
        }
    }

    col_struct.converted_count = numbers_v.size();
    auto write_ptr = col_struct.data;

    // Compress significant digits
    if (!numbers_v.empty()) {
        uint32_t used_space;
        IntegerSchemePicker::compress(numbers_v.data(), nullptr, write_ptr, numbers_v.size(),
                                      allowed_cascading_level - 1, used_space,
                                      col_struct.numbers_scheme, autoScheme(), "significant digits");
        write_ptr += used_space;
        log::codec().debug("Decimal: sd_c = {} sd_s = {}", col_struct.numbers_scheme, used_space);
    }

    // Compress exponents
    {
        col_struct.exponents_offset = write_ptr - col_struct.data;
        uint32_t used_space;
    //    SInteger32Stats e_stats =
    //        SInteger32Stats::generateStats(exponent_v.data(), nullptr, exponent_v.size());
        // cout << e_stats.min << '\t' << e_stats.max << endl;
   //     IntegerSchemePicker::compress(exponent_v.data(), nullptr, write_ptr, exponent_v.size(),
   //                                   allowed_cascading_level - 1, used_space,
    //                                  col_struct.exponents_scheme, autoScheme(), "exponents");
        write_ptr += used_space;
        log::codec().debug("Decimal: e_c = {} e_s = {}", col_struct.exponents_scheme, used_space);
    }

    // Compress patches
    {
        col_struct.patches_offset = write_ptr - col_struct.data;
        uint32_t used_space;
        DoubleSchemePicker::compress(patches_v.data(), nullptr, write_ptr, patches_v.size(),
                                     allowed_cascading_level - 1, used_space, col_struct.patches_scheme,
                                     autoScheme(), "patches");
        write_ptr += used_space;
        log::codec().debug("Decimal: p_c = {} p_s = {}", CI(col_struct.patches_scheme), CI(used_space));
    }

    // Write exceptions bitmap
    {
        col_struct.exceptions_map_offset = write_ptr - col_struct.data;
   //     exceptions_bitmap.runOptimize();
   //     exceptions_bitmap.setCopyOnWrite(true);
        write_ptr += exceptions_bitmap.write(reinterpret_cast<char*>(write_ptr), false);
    }

    return write_ptr - dest;
}

struct DecimalIterateParam {
    uint32_t next_block_i;
    uint32_t tuple_count;
    double* write_ptr;
    int32_t* exponents_ptr;
    int32_t* numbers_ptr;
    double* patches_ptr;
};

static inline void decompressExceptionBlock(DecimalIterateParam* param) {
    uint32_t row_start_i = param->next_block_i * block_size;
    uint32_t row_end_i = std::min(row_start_i + block_size, param->tuple_count);
    for (uint32_t row_i = row_start_i; row_i < row_end_i; row_i++) {
        int32_t exponent = *param->exponents_ptr++;
        if (exponent == exponent_exception_code) {
            *param->write_ptr++ = *param->patches_ptr++;
        } else {
            auto number = *param->numbers_ptr++;
            uint8_t exponent_index = exponent & decimal_index_mask;
            double original_double = static_cast<double>(number) * exact_fractions_of_ten[exponent_index];
            *param->write_ptr++ = original_double;
        }
    }
    param->next_block_i++;
}

void decompress(double* dest, BitmapWrapper*, const uint8_t* src, uint32_t tuple_count, uint32_t level) {
    // idea save exceptions in roaring in blocks of 4.
    // iterate over roaring. for everything up to the value us vectorized
    // implementation for anything value itself use non-vectorized impl don't
    // forget last block

    const auto& col_struct = *reinterpret_cast<const DecimalStructure*>(src);
    thread_local std::vector<std::vector<int32_t>> numbers_v;
    auto numbers_ptr =
        get_level_data(numbers_v, col_struct.converted_count + SIMD_EXTRA_ELEMENTS(int32_t), level);
    thread_local std::vector<std::vector<int32_t>> exponents_v;
    auto exponents_ptr =
        get_level_data(exponents_v, tuple_count + SIMD_EXTRA_ELEMENTS(int32_t), level);
    thread_local std::vector<std::vector<double>> patches_v;
    auto patches_ptr = get_level_data(
        patches_v, tuple_count - col_struct.converted_count + SIMD_EXTRA_ELEMENTS(double), level);
    Roaring exceptions_bitmap = Roaring::read(
        reinterpret_cast<const char*>(col_struct.data + col_struct.exceptions_map_offset), false);

    if (col_struct.converted_count > 0) {
        IntegerScheme& numbers_scheme =
            IntegerSchemePicker::MyTypeWrapper::getScheme(col_struct.numbers_scheme);
        numbers_scheme.decompress(numbers_v[level].data(), nullptr, col_struct.data,
                                  col_struct.converted_count, level + 1);
    }
    IntegerScheme& exponents_scheme =
        IntegerSchemePicker::MyTypeWrapper::getScheme(col_struct.exponents_scheme);
    exponents_scheme.decompress(exponents_v[level].data(), nullptr,
                                col_struct.data + col_struct.exponents_offset, tuple_count,
                                level + 1);

    DoubleScheme& patches_scheme =
        DoubleSchemePicker::MyTypeWrapper::getScheme(col_struct.patches_scheme);
    patches_scheme.decompress(patches_v[level].data(), nullptr,
                              col_struct.data + col_struct.patches_offset,
                              tuple_count - col_struct.converted_count, level + 1);


    auto write_ptr = dest;
    for (uint32_t row_i = 0; row_i < tuple_count; row_i++) {
        int32_t exponent = *exponents_ptr++;
        if (exponent == exponent_exception_code) {
            *write_ptr++ = *patches_ptr++;
        } else {
            auto number = *numbers_ptr++;
            uint8_t exponent_index = exponent & decimal_index_mask;
            double original_double = static_cast<double>(number) * exact_fractions_of_ten[exponent_index];
            *write_ptr++ = original_double;
        }
    }
}

bool isUsable(DoubleStats& stats) {
    double unique_ratio =
        static_cast<double>(stats.unique_count) / static_cast<double>(stats.tuple_count);
    if (unique_ratio < 0.1) {
        return false;
    }
    return true;
}
// -------------------------------------------------------------------------------------
}  // namespace btrblocks::doubles
// -------------------------------------------------------------------------------------