/*

Copyright (c) 2017, Schizofreny s.r.o - info@schizofreny.com
All rights reserved.

See LICENSE.md file

*/

#include "scalar.hpp"
#include "helpers.hpp"
#include <algorithm>
#include <cstdint>
#include <vector>
#include <memory>

namespace middleout {

template class Scalar<double>;
template class Scalar<int64_t>;
template class Scalar<uint64_t>;

template <typename T>
Scalar<T>::Scalar() {}

template <typename T>
std::unique_ptr<std::vector<char>> Scalar<T>::compressSimple(std::vector<T>& data) {
	std::unique_ptr<std::vector<char>> compressed(
	    new std::vector<char>(Scalar<T>::maxCompressedSize(data.size())));
	size_t size = Scalar<T>::compress(data, *compressed);
	compressed->resize(size);
	compressed->shrink_to_fit();
	return compressed;
}

/*

AVX 512 block compatible

*/
template <typename T>
size_t Scalar<T>::compress(std::vector<T>& data, std::vector<char>& output) {
	if (data.size() <= MIN_DATA_SIZE_COMPRESSION_TRESHOLD) {
		// not enough data to compress
		return doNotCompressTheData(data, output);
	}

	// skip first 8 init values
	size_t outputIndex = sizeof(int64_t) * VECTOR_SIZE;
	// "middle-out" data block size
	size_t blockSize = data.size() / VECTOR_SIZE;
	// just copy init reference values
	fillStart(data, output, blockSize);

	// main compression loop
	for (size_t i = 1; i < blockSize; i += 1) {
		uint8_t sameMask = 0;  // bit mask if current value is same as previous one
		int maxLength = 0;     // max (within 8 values) length of compressed value in bytes

		// offset within xored value is number of bytes from right where starts non-zero bits after
		// xored with the previous value (rounded down to bytes).
		// These values are 3bits and are stored (scattered) in 3 bottom bytes of this int
		uint32_t compressedOffsets = 0;

		// store xored and accordingly alligned (shifted) values in temp array
		// temp array allows perform compression logic wihout loop dependency
		uint64_t xoredShifted[8] = {0, 0, 0, 0, 0, 0, 0, 0};

		// if set, store index for next value will be shifted and stored data would not be overriten
		int dataStoreFlags[8] = {0, 0, 0, 0, 0, 0, 0, 0};

		uint32_t offsetsShift = 3;  // skip 3 bits for max length
		int notSameCount = 0;

		for (size_t j = 0; j < VECTOR_SIZE; j++) {
			// offset within input vector
			size_t offset = blockSize * j + i;
			// previous value - used for xor
			int64_t prev = reinterpret_cast<uint64_t&>(data[offset - 1]);
			int64_t curr = reinterpret_cast<uint64_t&>(data[offset]);

			// xore current value with previous
			int64_t xored = prev xor curr;

			// if data did not change
			if (xored == 0) {
				// is same as previous value
				// just set appropriate bit
				sameMask = sameMask | (1 << j);
				continue;
			}

			// this value will be stored
			dataStoreFlags[j] = 1;

			int leadingZeros = __builtin_clzl(xored);
			int trailingZeros = __builtin_ctzl(xored);

			int rightOffsetBits = floor8(trailingZeros);
			int rightOffsetBytes = trailingZeros >> 3;

			int leadingZerosBytes = leadingZeros >> 3;
			int trailingZerosBytes = trailingZeros >> 3;
			// compute length of non-zero data wihin xored value
			int lengthBytes = 8 - leadingZerosBytes - trailingZerosBytes;

			maxLength = std::max(lengthBytes, maxLength);

			// scatter current offset
			compressedOffsets |= rightOffsetBytes << offsetsShift;
			offsetsShift += 3;
			notSameCount++;

			// write value to tmp array aligned to right
			xoredShifted[j] = xored >> rightOffsetBits;
		}

		output[outputIndex++] = sameMask;

		// do not store max length and offsets if all values are the same
		if (sameMask == 0b11111111) {
			continue;
		}

		int* outAsInts = reinterpret_cast<int*>(&output[outputIndex]);
		// write 4 bytes with allready writed data and scattered offsets
		// -1 becasue we need to store only values 1-8, so 3 bits are enought
		outAsInts[0] = compressedOffsets | (maxLength - 1);

		// skip bytes poluted by offsets, rouded up to bytes
		// yes, we waste here some bits, but it is for the sake of performance
		// +1 because first 3 bits are maxLength
		outputIndex += getBytesLengthOfOffsets(notSameCount + 1);

		// write prepared data
		for (size_t j = 0; j < VECTOR_SIZE; j++) {
			T* outAsLongs = reinterpret_cast<T*>(&output[outputIndex]);

			outAsLongs[0] = reinterpret_cast<T&>(xoredShifted[j]);
			// shift index
			outputIndex += dataStoreFlags[j] * maxLength;
		}
	}

	// write rest of the data without any compression
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
inline void decompressValue(const size_t j,
                            const long blockSize,
                            std::vector<char>& input,
                            std::vector<T>& data,
                            uint64_t clearTopBitMask,
                            size_t* inputIndex,
                            const long i,
                            int* offsetsShift,
                            uint8_t maxLength,
                            uint32_t compresedOffsets,
                            uint8_t sameMask) {
	// middle-out offset
	size_t offset = blockSize * j + i;
	uint64_t prev = reinterpret_cast<uint64_t&>(data[offset - 1]);

	// get number of bits to shift xored data block
	// - shift to get current's block offset:   >> offsetsShift
	// - zero unused bits:                      & 0b111
	// - shift back to count in bits not bytes: << 3
	int shiftBits = ((compresedOffsets >> *offsetsShift) & 0b111) * 8;

	// get data from unaligned possition
	uint64_t toXor = reinterpret_cast<uint64_t*>(&input[*inputIndex])[0];

	// clear unused bytes
	toXor &= clearTopBitMask;

	// save result to variable, may not be usefull because of isSameMask bit
	uint64_t result = prev xor (toXor << shiftBits);

	// test bit on j-th possition
	int isSame = sameMask & (1 << j);
	// index would be moved by length of offsets in bytes
	int modifiedOffsetsShifts = *offsetsShift + 3;

	/*
	 Conditionally set final data (result of xor operation or previous value)
	 Conditionally increment inputIndex if data were consumed
	*/
	size_t newInputIndex = *inputIndex;
	size_t modifiedInputIndex = *inputIndex + maxLength;
	int newOffsetsShift = *offsetsShift;

	// make conditional assignmens as cmovs
	// compiler didn't took this optimalization (nor even on ternal operators)
	__asm(
	    "cmpl  $0, %[isSame];  	\t\n\
			cmove %[modifiedOffsetsShifts], %[newOffsetsShift]; \t\n\
			cmove %[result], %[prev];	\t\n\
			cmove %[modifiedInputIndex], %[newInputIndex];"
	    : [newOffsetsShift] "+r"(newOffsetsShift),             // offset shift final value
	      [prev] "+r"(prev),                                   // final value of data
	      [newInputIndex] "+r"(newInputIndex)                  // final value of index in stream
	    : [isSame] "r"(isSame),                                // anded mask
	      [modifiedOffsetsShifts] "r"(modifiedOffsetsShifts),  // offset as if stream was consumed
	      [result] "r"(result),                                // value as if stream was consumed
	      [modifiedInputIndex] "r"(modifiedInputIndex)  // offset of stream if values was consumed
	    : "cc"                                          // cmpl instructions sets cc flags
	    );

	// write final data
	data[offset] = reinterpret_cast<T&>(prev);
	// these assignments are optimized by compiler (happens in inline asm)
	*inputIndex = newInputIndex;
	*offsetsShift = newOffsetsShift;
}

template <bool CECK_FOR_ALL_SAME, typename T>
inline void decompressBlock(std::vector<char>& input,
                            std::vector<T>& data,
                            size_t* inputIndex,
                            const long blockSize,
                            const long i) {
	// read mask of same values
	uint8_t sameMask = input[(*inputIndex)++];
	size_t startInputIndex = *inputIndex;

	// check this only on a last few iteration (saves us a branching)
	// checking is for preventing access to unallocated data
	if (CECK_FOR_ALL_SAME) {
		if (sameMask == 0b11111111) {
			// just copy prev values
			for (size_t j = 0; j < VECTOR_SIZE; j++) {
				size_t offset = blockSize * j + i;
				data[offset] = data[offset - 1];
			}
			return;
		}
	}

	// read unaligned offsets
	uint32_t compresedOffsetsAndMaxLength = reinterpret_cast<uint32_t*>(&input[*inputIndex])[0];
	// +1 because only 3 bits are stored and valid lengths are 1-8
	uint8_t maxLength = (compresedOffsetsAndMaxLength & 0b111) + 1;

	// lookup table for next two lines is *not* faster
	int sameCount = __builtin_popcount(sameMask);
	// move input stream cursor by offsets header
	*inputIndex += getBytesLengthOfOffsets(VECTOR_SIZE - sameCount + 1);
	// cursor within offsets header
	int offsetsShift = 3;  // skip 3 bits for maxLength

	// set mask for clear upper bits before xoring
	uint64_t clearTopBitMask = ~((uint64_t)0) >> (64 - 8 * maxLength);

// manual unroll, some compilers do not like inline asm in body of loop for unroll
#define CALL_DECOMPRESS_VALUE(POSITION)                                               \
	decompressValue(POSITION, blockSize, input, data, clearTopBitMask, inputIndex, i, \
	                &offsetsShift, maxLength, compresedOffsetsAndMaxLength, sameMask);

	CALL_DECOMPRESS_VALUE(0)
	CALL_DECOMPRESS_VALUE(1)
	CALL_DECOMPRESS_VALUE(2)
	CALL_DECOMPRESS_VALUE(3)
	CALL_DECOMPRESS_VALUE(4)
	CALL_DECOMPRESS_VALUE(5)
	CALL_DECOMPRESS_VALUE(6)
	CALL_DECOMPRESS_VALUE(7)

	// variable for cmov result
	size_t inputIndexFinal = *inputIndex;
	/*
	prevent conditional jumps by cmov instruction
	Just revert input stream index if metadata would not been consumed
	*/
	__asm(
	    "cmpl  $0b11111111, %[sameMask];  	\t\n\
		cmove %[startInputIndex], %[inputIndexFinal];"
	    : [inputIndexFinal] "+r"(inputIndexFinal)
	    : [sameMask] "r"((int)sameMask), [startInputIndex] "r"(startInputIndex)
	    : "cc");

	*inputIndex = inputIndexFinal;
}

template <typename T>
void Scalar<T>::decompress(std::vector<char>& input, size_t inputElements, std::vector<T>& data) {
	// not enough data to compress
	if (inputElements <= MIN_DATA_SIZE_COMPRESSION_TRESHOLD) {
		return doNotDecompressTheData(input, inputElements, data);
	}

	// middle-out block size
	long blockSize = inputElements / VECTOR_SIZE;
	// copy first ref. values
	for (size_t i = 0; i < VECTOR_SIZE; i++) {
		data[blockSize * i] = (reinterpret_cast<T*>(input.data()))[i];
	}

	// skip first 8 init values
	size_t inputIndex = sizeof(int64_t) * VECTOR_SIZE;

	// main decompression loop
	long blockIndex = 1;
	// boundary check for last 5 values (there potentially could be 5 bytes read ahead, that means
	// max 5 blocks of data)
	for (; blockIndex < blockSize - 5; blockIndex++) {
		decompressBlock<false>(input, data, &inputIndex, blockSize, blockIndex);
	}
	for (; blockIndex < blockSize; blockIndex++) {
		// decompress last 5 blocks with boundary check (skip code if all elements are the same)
		decompressBlock<true>(input, data, &inputIndex, blockSize, blockIndex);
	}

	// copy rest of data (uncompressed)
	for (size_t i = blockSize * VECTOR_SIZE; i < inputElements; i++) {
		data[i] = (reinterpret_cast<T*>(&input[inputIndex]))[0];
		inputIndex += sizeof(int64_t);
	}
}

}  // end namespace middleout