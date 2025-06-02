/*
 * Adapted from http;s://github.com/cwida/ALP. See attached LICENSE file for details
 */

#ifndef ALP_CONFIG_HPP
#define ALP_CONFIG_HPP

#include <cstddef>

/*
 * ALP Configs
 */
namespace alp::config {
/// ALP Vector size (We recommend against changing this; it should be constant)
inline constexpr size_t VECTOR_SIZE = 1024;
/// number of vectors per rowgroup
inline constexpr size_t N_VECTORS_PER_ROWGROUP = 100UL;
/// Rowgroup size
inline constexpr size_t ROWGROUP_SIZE = N_VECTORS_PER_ROWGROUP * VECTOR_SIZE;
/// Vectors from the rowgroup from which to take samples; this will be used to then calculate the jumps
inline constexpr size_t ROWGROUP_VECTOR_SAMPLES = 8;
/// We calculate how many equidistant vector we must jump within a rowgroup
inline constexpr size_t ROWGROUP_SAMPLES_JUMP = (ROWGROUP_SIZE / ROWGROUP_VECTOR_SAMPLES) / VECTOR_SIZE;
/// Values to sample per vector
inline constexpr size_t SAMPLES_PER_VECTOR = 32;
/// Maximum number of combinations obtained from row group sampling
inline constexpr size_t MAX_K_COMBINATIONS     = 5;
inline constexpr size_t CUTTING_LIMIT          = 16;
inline constexpr size_t MAX_RD_DICT_BIT_WIDTH  = 3;
inline constexpr size_t MAX_RD_DICTIONARY_SIZE = (1 << MAX_RD_DICT_BIT_WIDTH);

} // namespace alp::config

#endif