/*

Copyright (c) 2017, Schizofreny s.r.o - info@schizofreny.com
All rights reserved.

See LICENSE.md file

*/

#include <vector>
#include <cstddef>
#include <cstdint>
#include <immintrin.h>
#include <iostream>

#ifndef HELPERS_H
#define HELPERS_H

namespace middleout {

// AVX512 vector of doubles
const size_t VECTOR_SIZE = 8;
const size_t MIN_DATA_SIZE_COMPRESSION_TRESHOLD = 2 * VECTOR_SIZE;

//
// INLINE FUNCTIONS
//

/*
 floors number to base of 8
*/
inline int floor8(int x) {
	return x & ~7;
}

/*
 ceils number to base of 8
*/
inline int ceil8(int x) {
	return (x + 7) & ~7;
}

inline uint64_t clearTopBits(uint64_t data, uint64_t bitsCount) {
	uint64_t clearBase = ~0;
	return data & (clearBase >> bitsCount);
}

inline int getBytesLengthOfOffsets(int offsetsCount) {
	// * 3 = bits per one offset
	// + 7 = round up to bytes
	// >>  = get number of bytes
	return (offsetsCount * 3 + 7) >> 3;
}

#ifdef USE_AVX512
inline __m512i clearTopBits(__m512i toClear, uint64_t bitsCount) {
	// uint64_t clearBase = ~0;
	// return data & (clearBase >> bitsCount);

	__m512i clearBase = _mm512_set1_epi64(~0);
	return _mm512_and_epi64(toClear, _mm512_srli_epi64(clearBase, bitsCount));
}
#endif

template <typename T>
void fillStart(std::vector<T>& data, std::vector<char>& output, size_t blockSize) {
	T* outAsLong = reinterpret_cast<T*>(output.data());
	for (size_t i = 0; i < VECTOR_SIZE; i++) {
		outAsLong[i] = data[blockSize * i];
	}
}

template <typename T>
size_t doNotCompressTheData(std::vector<T>& data, std::vector<char>& output) {
	auto asLong = reinterpret_cast<T*>(output.data());
	for (size_t i = 0; i < data.size(); i++) {
		asLong[i] = data[i];
	}
	return sizeof(T) * data.size();
}

template <typename T>
void doNotDecompressTheData(std::vector<char>& input, size_t inputElements, std::vector<T>& data) {
	auto asLong = reinterpret_cast<T*>(input.data());
	for (size_t i = 0; i < inputElements; i++) {
		data[i] = asLong[i];
	}
}

}  // end namespace middleout

#endif /* HELPERS_H */