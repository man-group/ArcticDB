#include <arcticdb/codec/bitmap_utils.hpp>

namespace arcticdb {

bm::serializer<bm::bvector<>>::buffer encode_bitmap(const util::BitSet& sparse_map) {
    bm::serializer<bm::bvector<> > bvs;
    bm::serializer<bm::bvector<> >::buffer buffer;
    bvs.serialize(sparse_map, buffer);
    return buffer;
}

} // namespace arcticdb