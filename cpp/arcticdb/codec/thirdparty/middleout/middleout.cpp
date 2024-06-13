/*

Copyright (c) 2017, Schizofreny s.r.o - info@schizofreny.com
All rights reserved.

See LICENSE.md file

*/

#include <vector>
#include <stdint.h>
#include <stdlib.h>
#include <memory>
#include "middleout.hpp"

#ifdef USE_AVX512
#include "avx512.hpp"
#define ALG_CLASS Avx52
#else
#include "scalar.hpp"
#define ALG_CLASS Scalar
#endif

namespace middleout {

std::unique_ptr<std::vector<char>> compressSimple(std::vector<int64_t>& data) {
	return ALG_CLASS<int64_t>::compressSimple(data);
}

std::unique_ptr<std::vector<char>> compressSimple(std::vector<double>& data) {
	return ALG_CLASS<double>::compressSimple(data);
}

size_t compress(std::vector<int64_t>& data, std::vector<char>& output) {
	return ALG_CLASS<int64_t>::compress(data, output);
}

size_t compress(std::vector<double>& data, std::vector<char>& output) {
	return ALG_CLASS<double>::compress(data, output);
}

void decompress(std::vector<char>& input, size_t inputElements, std::vector<int64_t>& data) {
	return ALG_CLASS<int64_t>::decompress(input, inputElements, data);
}

void decompress(std::vector<char>& input, size_t inputElements, std::vector<double>& data) {
	return ALG_CLASS<double>::decompress(input, inputElements, data);
}

size_t maxCompressedSize(size_t count) {
	return ALG_CLASS<double>::maxCompressedSize(count);
}

}  // end namespace middleout
