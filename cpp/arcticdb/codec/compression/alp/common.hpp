/*
 * Adapted from http;s://github.com/cwida/ALP. See attached LICENSE file for details
 */

#ifndef ALP_COMMON_HPP
#define ALP_COMMON_HPP

#include <cstdint>

namespace alp {
//! bitwidth type
using bw_t = uint8_t;
//! exception counter type
using exp_c_t = uint16_t;
//! exception position type
using exp_p_t = uint16_t;
//! factor idx  type
using factor_idx_t = uint8_t;
//! exponent idx type
using exponent_idx_t = uint8_t;
} // namespace alp

#endif // ALP_COMMON_HPP
