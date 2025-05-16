#include <gtest/gtest.h>

// This code adapted from the fastlanes project for the sake of binary and speed comparison
// https://github.com/cwida/FastLanes

static void unpack_11bw_32ow_32crw_1uf(const uint32_t* __restrict a_in_p, uint32_t* __restrict a_out_p) {
    [[maybe_unused]] auto       out = reinterpret_cast<uint32_t*>(a_out_p);
    [[maybe_unused]] const auto in  = reinterpret_cast<const uint32_t*>(a_in_p);
    [[maybe_unused]] uint32_t   register_0;
    [[maybe_unused]] uint32_t   tmp_0;
    [[maybe_unused]] uint32_t   base_0 = 0ULL;
    for (int i = 0; i < 32; ++i) {
        register_0                   = *(in + (0 * 32) + (i * 1) + 0);
        tmp_0                        = (register_0) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 0]  = tmp_0;
        tmp_0                        = (register_0 >> 11) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 32] = tmp_0;
        tmp_0                        = (register_0 >> 22) & ((1ULL << 10) - 1);
        register_0                   = *(in + (0 * 32) + (i * 1) + 32);
        tmp_0 |= ((register_0) & ((1ULL << 1) - 1)) << 10;
        out[(i * 1) + (0 * 32) + 64]  = tmp_0;
        tmp_0                         = (register_0 >> 1) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 96]  = tmp_0;
        tmp_0                         = (register_0 >> 12) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 128] = tmp_0;
        tmp_0                         = (register_0 >> 23) & ((1ULL << 9) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 64);
        tmp_0 |= ((register_0) & ((1ULL << 2) - 1)) << 9;
        out[(i * 1) + (0 * 32) + 160] = tmp_0;
        tmp_0                         = (register_0 >> 2) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 192] = tmp_0;
        tmp_0                         = (register_0 >> 13) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 224] = tmp_0;
        tmp_0                         = (register_0 >> 24) & ((1ULL << 8) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 96);
        tmp_0 |= ((register_0) & ((1ULL << 3) - 1)) << 8;
        out[(i * 1) + (0 * 32) + 256] = tmp_0;
        tmp_0                         = (register_0 >> 3) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 288] = tmp_0;
        tmp_0                         = (register_0 >> 14) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 320] = tmp_0;
        tmp_0                         = (register_0 >> 25) & ((1ULL << 7) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 128);
        tmp_0 |= ((register_0) & ((1ULL << 4) - 1)) << 7;
        out[(i * 1) + (0 * 32) + 352] = tmp_0;
        tmp_0                         = (register_0 >> 4) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 384] = tmp_0;
        tmp_0                         = (register_0 >> 15) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 416] = tmp_0;
        tmp_0                         = (register_0 >> 26) & ((1ULL << 6) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 160);
        tmp_0 |= ((register_0) & ((1ULL << 5) - 1)) << 6;
        out[(i * 1) + (0 * 32) + 448] = tmp_0;
        tmp_0                         = (register_0 >> 5) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 480] = tmp_0;
        tmp_0                         = (register_0 >> 16) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 512] = tmp_0;
        tmp_0                         = (register_0 >> 27) & ((1ULL << 5) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 192);
        tmp_0 |= ((register_0) & ((1ULL << 6) - 1)) << 5;
        out[(i * 1) + (0 * 32) + 544] = tmp_0;
        tmp_0                         = (register_0 >> 6) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 576] = tmp_0;
        tmp_0                         = (register_0 >> 17) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 608] = tmp_0;
        tmp_0                         = (register_0 >> 28) & ((1ULL << 4) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 224);
        tmp_0 |= ((register_0) & ((1ULL << 7) - 1)) << 4;
        out[(i * 1) + (0 * 32) + 640] = tmp_0;
        tmp_0                         = (register_0 >> 7) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 672] = tmp_0;
        tmp_0                         = (register_0 >> 18) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 704] = tmp_0;
        tmp_0                         = (register_0 >> 29) & ((1ULL << 3) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 256);
        tmp_0 |= ((register_0) & ((1ULL << 8) - 1)) << 3;
        out[(i * 1) + (0 * 32) + 736] = tmp_0;
        tmp_0                         = (register_0 >> 8) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 768] = tmp_0;
        tmp_0                         = (register_0 >> 19) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 800] = tmp_0;
        tmp_0                         = (register_0 >> 30) & ((1ULL << 2) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 288);
        tmp_0 |= ((register_0) & ((1ULL << 9) - 1)) << 2;
        out[(i * 1) + (0 * 32) + 832] = tmp_0;
        tmp_0                         = (register_0 >> 9) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 864] = tmp_0;
        tmp_0                         = (register_0 >> 20) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 896] = tmp_0;
        tmp_0                         = (register_0 >> 31) & ((1ULL << 1) - 1);
        register_0                    = *(in + (0 * 32) + (i * 1) + 320);
        tmp_0 |= ((register_0) & ((1ULL << 10) - 1)) << 1;
        out[(i * 1) + (0 * 32) + 928] = tmp_0;
        tmp_0                         = (register_0 >> 10) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 960] = tmp_0;
        tmp_0                         = (register_0 >> 21) & ((1ULL << 11) - 1);
        out[(i * 1) + (0 * 32) + 992] = tmp_0;
    }
}

void static pack_11bit_32ow(const uint32_t* __restrict in, uint32_t* __restrict out) {
    uint32_t tmp = 0U;
    uint32_t src;
    for (int i = 0; i < 32; i++) {
        src = *(in + 32 * 0 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src;
        src = *(in + 32 * 1 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 11U;
        src = *(in + 32 * 2 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 22U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 2 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 10U;
        src = *(in + 32 * 3 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 1U;
        src = *(in + 32 * 4 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 12U;
        src = *(in + 32 * 5 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 23U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 5 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 9U;
        src = *(in + 32 * 6 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 2U;
        src = *(in + 32 * 7 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 13U;
        src = *(in + 32 * 8 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 24U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 8 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 8U;
        src = *(in + 32 * 9 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 3U;
        src = *(in + 32 * 10 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 14U;
        src = *(in + 32 * 11 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 25U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 11 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 7U;
        src = *(in + 32 * 12 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 4U;
        src = *(in + 32 * 13 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 15U;
        src = *(in + 32 * 14 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 26U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 14 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 6U;
        src = *(in + 32 * 15 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 5U;
        src = *(in + 32 * 16 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 16U;
        src = *(in + 32 * 17 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 27U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 17 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 5U;
        src = *(in + 32 * 18 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 6U;
        src = *(in + 32 * 19 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 17U;
        src = *(in + 32 * 20 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 28U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 20 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 4U;
        src = *(in + 32 * 21 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 7U;
        src = *(in + 32 * 22 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 18U;
        src = *(in + 32 * 23 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 29U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 23 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 3U;
        src = *(in + 32 * 24 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 8U;
        src = *(in + 32 * 25 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 19U;
        src = *(in + 32 * 26 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 30U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 26 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 2U;
        src = *(in + 32 * 27 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 9U;
        src = *(in + 32 * 28 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 20U;
        src = *(in + 32 * 29 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 31U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 29 + i);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 1U;
        src = *(in + 32 * 30 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 10U;
        src = *(in + 32 * 31 + i);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 21U;
        *(out + i) = tmp;
        out -= 320;
    }
}

TEST(Fastlanes, OriginalPackUnpack32to11) {
    constexpr int blocksPerColumn = 100;
    constexpr int blockInSize = 1024;    // each block: 1024 integers (32x32)
    constexpr int blockOutSize = 352;    // each pack call writes 320 integers
    constexpr int totalInSize = blocksPerColumn * blockInSize;
    constexpr int totalPackedSize = blocksPerColumn * blockOutSize;

    std::vector<uint32_t> in(totalInSize);
    for (int i = 0; i < totalInSize; ++i) {
        in[i] = i & ((1 << 11) - 1); // ensure values fit in 11 bits
    }

    std::vector<uint32_t> packed(totalPackedSize, 0);
    std::vector<uint32_t> unpacked(totalInSize, 0);

    for (int b = 0; b < blocksPerColumn; b++) {
        pack_11bit_32ow(in.data() + b * blockInSize, packed.data() + b * blockOutSize);
    }

    constexpr int iterations = 10000;
    uint32_t pack_checksum = 0;
    auto start_pack = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iterations; iter++) {
        for (int b = 0; b < blocksPerColumn; b++) {
            pack_11bit_32ow(in.data() + b * blockInSize, packed.data() + b * blockOutSize);
            pack_checksum += packed[b * blockOutSize]; // accumulate first element from each block
        }
    }
    auto end_pack = std::chrono::high_resolution_clock::now();
    auto totalPackNanosecs = std::chrono::duration_cast<std::chrono::nanoseconds>(end_pack - start_pack).count();
    double avgPackTimePerColumn = static_cast<double>(totalPackNanosecs) / iterations;

    std::cout << "Pack Test:\n";
    std::cout << "  Iterations: " << iterations << "\n";
    std::cout << "  Total time (ns): " << totalPackNanosecs << "\n";
    std::cout << "  Average time per column (100*1024 integers): "
              << avgPackTimePerColumn << " ns\n";
    std::cout << "  Pack Checksum: " << pack_checksum << "\n\n";

    for (int b = 0; b < blocksPerColumn; b++) {
        unpack_11bw_32ow_32crw_1uf(packed.data() + b * blockOutSize,
                                   unpacked.data() + b * blockInSize);
    }

    uint32_t unpack_checksum = 0;
    auto start_unpack = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iterations; iter++) {
        for (int b = 0; b < blocksPerColumn; b++) {
            unpack_11bw_32ow_32crw_1uf(packed.data() + b * blockOutSize,
                                       unpacked.data() + b * blockInSize);
            unpack_checksum += unpacked[b * blockInSize]; // accumulate first element from each block
        }
    }
    auto end_unpack = std::chrono::high_resolution_clock::now();
    auto totalUnpackNanosecs = std::chrono::duration_cast<std::chrono::nanoseconds>(end_unpack - start_unpack).count();
    double avgUnpackTimePerColumn = static_cast<double>(totalUnpackNanosecs) / iterations;

    std::cout << "Unpack Test:\n";
    std::cout << "  Iterations: " << iterations << "\n";
    std::cout << "  Total time (ns): " << totalUnpackNanosecs << "\n";
    std::cout << "  Average time per column (100*1024 integers): "
              << avgUnpackTimePerColumn << " ns\n";
    std::cout << "  Unpack Checksum: " << unpack_checksum << "\n";

}

void static ffor_11bit_32ow(const uint32_t* __restrict in,
                            uint32_t* __restrict out,
                            const uint32_t* __restrict a_base_p) {
    uint32_t tmp = 0U;
    uint32_t src;
    for (int i = 0; i < 32; i++) {
        src = *(in + 32 * 0 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src;
        src = *(in + 32 * 1 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 11U;
        src = *(in + 32 * 2 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 22U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 2 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 10U;
        src = *(in + 32 * 3 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 1U;
        src = *(in + 32 * 4 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 12U;
        src = *(in + 32 * 5 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 23U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 5 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 9U;
        src = *(in + 32 * 6 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 2U;
        src = *(in + 32 * 7 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 13U;
        src = *(in + 32 * 8 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 24U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 8 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 8U;
        src = *(in + 32 * 9 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 3U;
        src = *(in + 32 * 10 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 14U;
        src = *(in + 32 * 11 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 25U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 11 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 7U;
        src = *(in + 32 * 12 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 4U;
        src = *(in + 32 * 13 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 15U;
        src = *(in + 32 * 14 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 26U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 14 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 6U;
        src = *(in + 32 * 15 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 5U;
        src = *(in + 32 * 16 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 16U;
        src = *(in + 32 * 17 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 27U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 17 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 5U;
        src = *(in + 32 * 18 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 6U;
        src = *(in + 32 * 19 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 17U;
        src = *(in + 32 * 20 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 28U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 20 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 4U;
        src = *(in + 32 * 21 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 7U;
        src = *(in + 32 * 22 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 18U;
        src = *(in + 32 * 23 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 29U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 23 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 3U;
        src = *(in + 32 * 24 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 8U;
        src = *(in + 32 * 25 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 19U;
        src = *(in + 32 * 26 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 30U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 26 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 2U;
        src = *(in + 32 * 27 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 9U;
        src = *(in + 32 * 28 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 20U;
        src = *(in + 32 * 29 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 31U;
        *(out + i) = tmp;
        out += 32;
        src = *(in + 32 * 29 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp = src >> 1U;
        src = *(in + 32 * 30 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 10U;
        src = *(in + 32 * 31 + i) - *(a_base_p);
        src = src & ((1ULL << 11) - 1);
        tmp |= src << 21U;
        *(out + i) = tmp;
        out -= 320;
    }
}

static void unffor_11bw_32ow_32crw_1uf(const uint32_t* __restrict a_in_p,
                                       uint32_t* __restrict a_out_p,
                                       const uint32_t* __restrict a_base_p) {
    [[maybe_unused]] auto       out = reinterpret_cast<uint32_t*>(a_out_p);
    [[maybe_unused]] const auto in  = reinterpret_cast<const uint32_t*>(a_in_p);
    [[maybe_unused]] uint32_t   register_0;
    [[maybe_unused]] uint32_t   tmp_0;
    [[maybe_unused]] uint32_t   base_0 = *(a_base_p);
    for (int i = 0; i < 32; ++i) {
        register_0 = *(in + (0 * 32) + (i * 1) + 0);
        tmp_0      = (register_0) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 0)) = tmp_0;
        tmp_0                                  = (register_0 >> 11) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 1)) = tmp_0;
        tmp_0                                  = (register_0 >> 22) & ((1ULL << 10) - 1);
        register_0                             = *(in + (0 * 32) + (i * 1) + 32);
        tmp_0 |= ((register_0) & ((1ULL << 1) - 1)) << 10;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 2)) = tmp_0;
        tmp_0                                  = (register_0 >> 1) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 3)) = tmp_0;
        tmp_0                                  = (register_0 >> 12) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 4)) = tmp_0;
        tmp_0                                  = (register_0 >> 23) & ((1ULL << 9) - 1);
        register_0                             = *(in + (0 * 32) + (i * 1) + 64);
        tmp_0 |= ((register_0) & ((1ULL << 2) - 1)) << 9;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 5)) = tmp_0;
        tmp_0                                  = (register_0 >> 2) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 6)) = tmp_0;
        tmp_0                                  = (register_0 >> 13) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 7)) = tmp_0;
        tmp_0                                  = (register_0 >> 24) & ((1ULL << 8) - 1);
        register_0                             = *(in + (0 * 32) + (i * 1) + 96);
        tmp_0 |= ((register_0) & ((1ULL << 3) - 1)) << 8;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 8)) = tmp_0;
        tmp_0                                  = (register_0 >> 3) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 9)) = tmp_0;
        tmp_0                                  = (register_0 >> 14) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 10)) = tmp_0;
        tmp_0                                   = (register_0 >> 25) & ((1ULL << 7) - 1);
        register_0                              = *(in + (0 * 32) + (i * 1) + 128);
        tmp_0 |= ((register_0) & ((1ULL << 4) - 1)) << 7;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 11)) = tmp_0;
        tmp_0                                   = (register_0 >> 4) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 12)) = tmp_0;
        tmp_0                                   = (register_0 >> 15) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 13)) = tmp_0;
        tmp_0                                   = (register_0 >> 26) & ((1ULL << 6) - 1);
        register_0                              = *(in + (0 * 32) + (i * 1) + 160);
        tmp_0 |= ((register_0) & ((1ULL << 5) - 1)) << 6;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 14)) = tmp_0;
        tmp_0                                   = (register_0 >> 5) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 15)) = tmp_0;
        tmp_0                                   = (register_0 >> 16) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 16)) = tmp_0;
        tmp_0                                   = (register_0 >> 27) & ((1ULL << 5) - 1);
        register_0                              = *(in + (0 * 32) + (i * 1) + 192);
        tmp_0 |= ((register_0) & ((1ULL << 6) - 1)) << 5;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 17)) = tmp_0;
        tmp_0                                   = (register_0 >> 6) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 18)) = tmp_0;
        tmp_0                                   = (register_0 >> 17) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 19)) = tmp_0;
        tmp_0                                   = (register_0 >> 28) & ((1ULL << 4) - 1);
        register_0                              = *(in + (0 * 32) + (i * 1) + 224);
        tmp_0 |= ((register_0) & ((1ULL << 7) - 1)) << 4;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 20)) = tmp_0;
        tmp_0                                   = (register_0 >> 7) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 21)) = tmp_0;
        tmp_0                                   = (register_0 >> 18) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 22)) = tmp_0;
        tmp_0                                   = (register_0 >> 29) & ((1ULL << 3) - 1);
        register_0                              = *(in + (0 * 32) + (i * 1) + 256);
        tmp_0 |= ((register_0) & ((1ULL << 8) - 1)) << 3;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 23)) = tmp_0;
        tmp_0                                   = (register_0 >> 8) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 24)) = tmp_0;
        tmp_0                                   = (register_0 >> 19) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 25)) = tmp_0;
        tmp_0                                   = (register_0 >> 30) & ((1ULL << 2) - 1);
        register_0                              = *(in + (0 * 32) + (i * 1) + 288);
        tmp_0 |= ((register_0) & ((1ULL << 9) - 1)) << 2;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 26)) = tmp_0;
        tmp_0                                   = (register_0 >> 9) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 27)) = tmp_0;
        tmp_0                                   = (register_0 >> 20) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 28)) = tmp_0;
        tmp_0                                   = (register_0 >> 31) & ((1ULL << 1) - 1);
        register_0                              = *(in + (0 * 32) + (i * 1) + 320);
        tmp_0 |= ((register_0) & ((1ULL << 10) - 1)) << 1;
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 29)) = tmp_0;
        tmp_0                                   = (register_0 >> 10) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 30)) = tmp_0;
        tmp_0                                   = (register_0 >> 21) & ((1ULL << 11) - 1);
        tmp_0 += base_0;
        *(out + (i * 1) + (0 * 32) + (32 * 31)) = tmp_0;
    }
}

TEST(FFORPerformanceTest, CompressionAndDecompressionTiming) {
    constexpr int blocksPerColumn = 100;
    constexpr int blockInSize = 1024;
    constexpr int blockOutSize = 353;
    constexpr int totalInSize = blocksPerColumn * blockInSize;
    constexpr int totalOutSize = blocksPerColumn * blockOutSize;
    constexpr int iterations = 10000;

    std::vector<uint32_t> in(totalInSize);
    std::vector<uint32_t> packed(totalOutSize, 0);
    std::vector<uint32_t> unpacked(totalInSize, 0);

    for (int i = 0; i < totalInSize; ++i) {
        in[i] = i & ((1 << 11) - 1);
    }

    uint32_t base = 0;
    const uint32_t* base_ptr = &base;

    for (int b = 0; b < blocksPerColumn; ++b) {
        ffor_11bit_32ow(in.data() + b * blockInSize,
                        packed.data() + b * blockOutSize,
                        base_ptr);
    }

    uint32_t pack_checksum = 0;  
    auto start_pack = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iterations; ++iter) {
        for (int b = 0; b < blocksPerColumn; ++b) {
            ffor_11bit_32ow(in.data() + b * blockInSize,
                            packed.data() + b * blockOutSize,
                            base_ptr);
            pack_checksum += packed[b * blockOutSize];
        }
    }
    auto end_pack = std::chrono::high_resolution_clock::now();
    auto pack_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_pack - start_pack).count();
    double avg_pack_time = static_cast<double>(pack_duration_ns) / iterations;

    std::cout << "FFOR Pack Performance:\n"
              << "  Iterations: " << iterations << "\n"
              << "  Total pack time (ns): " << pack_duration_ns << "\n"
              << "  Average pack time per column: " << avg_pack_time << " ns\n"
              << "  Pack Checksum: " << pack_checksum << "\n";

    for (int b = 0; b < blocksPerColumn; ++b) {
        unffor_11bw_32ow_32crw_1uf(
            packed.data() + b * blockOutSize,
            unpacked.data() + b * blockInSize,
            base_ptr);
    }

    uint32_t unpack_checksum = 0;
    auto start_unpack = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iterations; ++iter) {
        for (int b = 0; b < blocksPerColumn; ++b) {
            unffor_11bw_32ow_32crw_1uf(
                packed.data() + b * blockOutSize,
                unpacked.data() + b * blockInSize,
                base_ptr);
            unpack_checksum += unpacked[b * blockInSize];
        }
    }
    auto end_unpack = std::chrono::high_resolution_clock::now();
    auto unpack_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_unpack - start_unpack).count();
    double avg_unpack_time = static_cast<double>(unpack_duration_ns) / iterations;

    std::cout << "UnFFOR Unpack Performance:\n"
              << "  Iterations: " << iterations << "\n"
              << "  Total unpack time (ns): " << unpack_duration_ns << "\n"
              << "  Average unpack time per column: " << avg_unpack_time << " ns\n"
              << "  Unpack Checksum: " << unpack_checksum << "\n";

    EXPECT_NE(pack_checksum, 0u);
    EXPECT_NE(unpack_checksum, 0u);
}