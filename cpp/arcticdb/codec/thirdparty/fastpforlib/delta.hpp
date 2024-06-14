#include <cstddef>

// Based on https://github.com/lemire/FastPFor (Apache License 2.0)
template <class T>
static T DeltaDecode(T *data, T previous_value, const size_t size) {
   // D_ASSERT(size >= 1);

    data[0] += previous_value;

    const size_t UnrollQty = 4;
    const size_t sz0 = (size / UnrollQty) * UnrollQty; // equal to 0, if size < UnrollQty
    size_t i = 1;
    if (sz0 >= UnrollQty) {
        T a = data[0];
        for (; i < sz0 - UnrollQty; i += UnrollQty) {
            a = data[i] += a;
            a = data[i + 1] += a;
            a = data[i + 2] += a;
            a = data[i + 3] += a;
        }
    }
    for (; i != size; ++i) {
        data[i] += data[i - 1];
    }

    return data[size - 1];
}