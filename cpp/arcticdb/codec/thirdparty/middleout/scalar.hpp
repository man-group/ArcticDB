/*

Copyright (c) 2017, Schizofreny s.r.o - info@schizofreny.com
All rights reserved.

See LICENSE.md file

*/

#include <vector>
#include <cstddef>
#include <type_traits>
#include <memory>

#ifndef SCALAR2_H
#define SCALAR2_H

namespace middleout {

template <typename T>

class Scalar {
	static_assert(sizeof(T) == 8, "Must use datatype with length of 8 bytes.");

   public:
	Scalar();

	static std::unique_ptr<std::vector<char>> compressSimple(std::vector<T>& data);

	static size_t compress(std::vector<T>& data, std::vector<char>& output);

	static void decompress(std::vector<char>& input, size_t itemsCount, std::vector<T>& data);

	static size_t maxCompressedSize(size_t count) {
		size_t blockCount = count / 8;
		// 8*8            : init reference values
		// 5*blockClount  : max size of block headers
		// 8*8*blockCount : max size of xored data: 8 bytes * 8 values
		// 8*(count%8)    : uncompressed rest of values
		return 8 * 8 + blockCount * 5 + 8 * 8 * blockCount + 8 * (count % 8);
	}
};

}  // end namespace middleout

#endif /* SCALAR2_H */