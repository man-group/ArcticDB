#include <arcticdb/util/bitset.hpp>
#include <bitmagic/bmserial.h>

namespace arcticdb {

bm::serializer<bm::bvector<> >::buffer encode_bitmap(const util::BitSet& sparse_map);

}
