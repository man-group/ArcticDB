/*

Copyright (c) 2017, Schizofreny s.r.o - info@schizofreny.com
All rights reserved.

See LICENSE.md file

*/

#include "avx512.hpp"
#include "helpers.hpp"
#include <algorithm>
#include <cstdint>
#include <vector>
#include <immintrin.h>
#include <memory>

namespace middleout {

template class Avx52<double>;
template class Avx52<int64_t>;
template class Avx52<uint64_t>;

template <typename T>
Avx52<T>::Avx52() {}

template <typename T>
std::unique_ptr<std::vector<char>> Avx52<T>::compressSimple(std::vector<T>& data) {
	std::unique_ptr<std::vector<char>> compressed(
	    new std::vector<char>(Avx52<T>::maxCompressedSize(data.size())));
	size_t size = Avx52<T>::compress(data, *compressed);
	compressed->resize(size);
	compressed->shrink_to_fit();
	return compressed;
}

/**
 * return length in bytes within left zeros and right zeros
 */
inline __m512i byteLength(__mmask8 mask, __m512i leftOffset, __m512i rightOffset) {
	// return 8 - (leftOffset + rightOffset);
	__m512i baseLength = _mm512_set1_epi64(8);
	__m512i offsetsSum = _mm512_add_epi64(leftOffset, rightOffset);
	return _mm512_maskz_sub_epi64(mask, baseLength, offsetsSum);
}
/**
 * From number of bits, computes number of bytes (bytes). E.g. bits/8
 */
inline __m512i byteRound(__m512i x) {
	//	return x >> 3;
	return _mm512_srli_epi64(x, 3);
}

/**
 * Compresses offsets. Skip offsets by mask. Using 3bits per one offset
 */
inline int compressOffsets(__mmask8 notSame, __m512i offsets) {
	// shift each offset to correct position
	__m512i compressed = _mm512_maskz_compress_epi64(notSame, offsets);
	// shift within one vector to final position
	// skip first right 3 bits for maxLength
	__m512i positioned =
	    _mm512_sllv_epi64(compressed, _mm512_setr_epi64(3, 6, 9, 12, 15, 18, 21, 24));
	// combine positioned data with OR
	return (int)_mm512_reduce_or_epi64(positioned);
}

/**
 * Reverses bytes within elements of vector
 */
inline __m512i byte_reverse_within_epi64(__m512i a) {
	// set *EPI16* positions for permutations
	__m512i idx = _mm512_set_epi32(28 << 16 | 29, 30 << 16 | 31,  //
	                               24 << 16 | 25, 26 << 16 | 27,  //
	                               20 << 16 | 21, 22 << 16 | 23,  //
	                               16 << 16 | 17, 18 << 16 | 19,  //
	                               12 << 16 | 13, 14 << 16 | 15,  //
	                               8 << 16 | 9, 10 << 16 | 11,    //
	                               4 << 16 | 5, 6 << 16 | 7,      //
	                               0 << 16 | 1, 2 << 16 | 3);

	// in g++ set_epi16 is not defined
	// __m512i idx = _mm512_set_epi16(28, 29, 30, 31,  //
	//                                24, 25, 26, 27,  //
	//                                20, 21, 22, 23,  //
	//                                16, 17, 18, 19,  //
	//                                12, 13, 14, 15,  //
	//                                8, 9, 10, 11,    //
	//                                4, 5, 6, 7,      //
	//                                0, 1, 2, 3);

	// 2var because "1var" does not exists
	// permute double bytes
	__m512i permuted16 = _mm512_permutex2var_epi16(a, idx, a);
	// swap bytes within epi16
	__m512i right = _mm512_srli_epi16(permuted16, 8);
	__m512i left = _mm512_slli_epi16(permuted16, 8);

	// combine swaps
	return _mm512_or_epi32(right, left);
}

/**
 * Comress block of data
 */
template <typename T>
inline void compressBlock(std::vector<T>& data,
                          std::vector<char>& output,
                          size_t,  // size of one middle-out block
                          size_t* outputIndex,
                          const size_t i,        // position within middle-out block
                          const __m256i vindex,  // indexes within middle-out block
                          __m512i* prev) {
	__m512i curr = _mm512_i32gather_epi64(vindex, &data[i], 8);

	__m512i xored = _mm512_xor_epi64(*prev, curr);
	__mmask8 notSame = _mm512_cmp_epi64_mask(xored, _mm512_set1_epi64(0), _MM_CMPINT_NE);

	// store "not same" metadata
	output[(*outputIndex)++] = ~notSame;

	// test if whole mask is zeros
	if (notSame == 0) {
		// skip if all values are the same as previous
		return;
	}

	// computes 1s in mask - number of different values
	int notSameCount = __builtin_popcount(notSame);

	__m512i leadingZeros = _mm512_maskz_lzcnt_epi64(notSame, xored);
	__m512i trailingZeros = _mm512_maskz_lzcnt_epi64(notSame, byte_reverse_within_epi64(xored));

	__m512i leftOffsetBytes = byteRound(leadingZeros);
	__m512i rightOffsetBytes = byteRound(trailingZeros);

	__m512i lengthBytes = byteLength(notSame, leftOffsetBytes, rightOffsetBytes);

	uint8_t maxLength = (uint8_t)_mm512_reduce_max_epi64(lengthBytes);

	// right bytes offset * 8
	__m512i shiftsBitsFromRight = _mm512_slli_epi64(rightOffsetBytes, 3);
	// align non-zero xored part to right
	__m512i shiftedXored = _mm512_srlv_epi64(xored, shiftsBitsFromRight);

	int* outAsInts = reinterpret_cast<int*>(&output[*outputIndex]);
	// store offsets and max length
	// -1 becasue we need to store only values 1-8, so 3 bits are enought
	outAsInts[0] = compressOffsets(notSame, rightOffsetBytes) | (maxLength - 1);

	// +1 because first 3 bits are maxLength
	*outputIndex += getBytesLengthOfOffsets(notSameCount + 1);

	// maxLength * position
	__m512i storeBase =
	    _mm512_mullo_epi64(_mm512_set1_epi64(maxLength), _mm512_setr_epi64(0, 1, 2, 3, 4, 5, 6, 7));

	// compress within vector - store is done in strong order (documented behavior)
	// compression is done by data overlapping
	__m512i compressedXoredShifted = _mm512_maskz_compress_epi64(notSame, shiftedXored);
	// store data
	_mm512_i64scatter_epi64(&output[*outputIndex], storeBase, compressedXoredShifted, 1);

	// // shift output data offsets
	*outputIndex += notSameCount * maxLength;

	// preserve previous data in vector register
	*prev = curr;
}

/*

Middle-out compression

*/
template <typename T>
size_t Avx52<T>::compress(std::vector<T>& data, std::vector<char>& output) {
	if (data.size() <= MIN_DATA_SIZE_COMPRESSION_TRESHOLD) {
		// not enough data to compress
		return doNotCompressTheData(data, output);
	}

	// skip first 8 init values
	size_t outputIndex = sizeof(T) * VECTOR_SIZE;
	// "middle-out" data block size
	size_t blockSize = data.size() / VECTOR_SIZE;
	// just copy init reference values
	fillStart(data, output, blockSize);

	__m256i vindex = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
	vindex = _mm256_mullo_epi32(vindex, _mm256_set1_epi32(blockSize));

	__m512i prev = _mm512_i32gather_epi64(vindex, &data[0], 8);

	// main compression loop
	for (size_t i = 1; i < blockSize; i += 1) {
		compressBlock(data, output, blockSize, &outputIndex, i, vindex, &prev);
	}

	// write rest data without any compression
	for (size_t i = blockSize * VECTOR_SIZE; i < data.size(); i++) {
		T* outAsLongs = reinterpret_cast<T*>(&output[outputIndex]);
		outAsLongs[0] = data[i];
		outputIndex += sizeof(T);
	}

	// write compress algorithm version and datatype constant
	output[outputIndex++] = 0x7E;  //== 0b01111110

	// to avoid access to invalid memory on decompression
	return outputIndex + 6;
}

//
// DECOMPRESSION
//
template <typename T>
inline void decompressBlock(std::vector<char>& input,
                            size_t,
                            std::vector<T>& data,
                            size_t* inputIndex,      // position within input data
                            const size_t,  // size of middle-out block
                            const size_t i,          // position within block
                            const __m256i vindex,    // vector of output data indexes
                            __m512i* prev) {
	// read mask of same values
	uint8_t sameMask = input[(*inputIndex)++];

	if (sameMask == 0b11111111) {
		// all values are the same as previous ones
		// simple store prev elements
		_mm512_i32scatter_epi64(&data[i], vindex, *prev, 8);
		return;
	}

	__mmask8 notSameMask = ~sameMask;

	// read unaligned offsets, where offset = number of empty bytes from right in XORed value
	uint32_t compresedOffsetsAndMaxLength = reinterpret_cast<uint32_t*>(&input[*inputIndex])[0];
	// +1 because only 3 bits are stored and valid lengths are 1-8
	uint8_t maxLength = (compresedOffsetsAndMaxLength & 0b111) + 1;

	// count of 1s in mask
	int sameCount = __builtin_popcount(sameMask);
	int notSameCount = 8 - sameCount;

	// +1 because first 3 bits are maxLength
	*inputIndex += getBytesLengthOfOffsets(notSameCount + 1);

	__m256i decompressOffsets =
	    _mm256_maskz_expand_epi32(notSameMask, _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7));
	__m256i readShifts = _mm256_mullo_epi32(_mm256_set1_epi32(maxLength), decompressOffsets);

	__m512i toXor =
	    _mm512_mask_i32gather_epi64(*prev, notSameMask, readShifts, &input[*inputIndex], 1);

	// expand offsets to each positon within offsets vector
	__m512i offsets = _mm512_set1_epi64(compresedOffsetsAndMaxLength);
	// zero other offsets
	offsets = _mm512_and_epi64(offsets, _mm512_setr_epi64(0b111000,                      //
	                                                      0b111000000,                   //
	                                                      0b111000000000,                //
	                                                      0b111000000000000,             //
	                                                      0b111000000000000000,          //
	                                                      0b111000000000000000000,       //
	                                                      0b111000000000000000000000,    //
	                                                      0b111000000000000000000000000  //
	                                                      ));
	// set offsets back to right in vector elemet
	offsets = _mm512_srlv_epi64(offsets, _mm512_setr_epi64(3, 6, 9, 12, 15, 18, 21, 24));
	// get offsets to positions corresponding with compressed values
	offsets = _mm512_maskz_expand_epi64(notSameMask, offsets);
	// offset *8
	offsets = _mm512_slli_epi64(offsets, 3);
	toXor = clearTopBits(toXor, (64 - 8 * maxLength));

	// position to xor
	toXor = _mm512_sllv_epi64(toXor, offsets);
	__m512i xored = _mm512_mask_xor_epi64(*prev, notSameMask, *prev, toXor);

	_mm512_i32scatter_epi64(&data[i], vindex, xored, 8);

	*inputIndex += (sizeof(T) - sameCount) * maxLength;
	*prev = xored;
}

template <typename T>
void Avx52<T>::decompress(std::vector<char>& input, size_t inputElements, std::vector<T>& data) {
	// not enough data to compress
	if (inputElements <= MIN_DATA_SIZE_COMPRESSION_TRESHOLD) {
		return doNotDecompressTheData(input, inputElements, data);
	}

	//"middle-out" block size
	size_t blockSize = inputElements / VECTOR_SIZE;
	// copy first ref. values
	for (size_t i = 0; i < VECTOR_SIZE; i++) {
		data[blockSize * i] = (reinterpret_cast<T*>(input.data()))[i];
	}

	// skip first 8 init values
	size_t inputIndex = sizeof(T) * VECTOR_SIZE;

	// precompute vindex
	__m256i vindex = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
	vindex = _mm256_mullo_epi32(vindex, _mm256_set1_epi32(blockSize));

	__m512i prev = _mm512_loadu_si512(&input[0]);

	// main decompression loop
	for (size_t i = 1; i < blockSize; i += 1) {
		decompressBlock(input, inputElements, data, &inputIndex, blockSize, i, vindex, &prev);
	}

	// copy rest of data (uncompressed)
	for (size_t i = blockSize * VECTOR_SIZE; i < inputElements; i++) {
		data[i] = (reinterpret_cast<T*>(&input[inputIndex]))[0];
		inputIndex += sizeof(T);
	}
}

}  // end namespace middleout