/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <queue>

namespace arcticdb {

template<
        typename _Tp, typename _Sequence = std::vector<_Tp>,
        typename _Compare = std::less<typename _Sequence::value_type>>
class movable_priority_queue : std::priority_queue<_Tp, _Sequence, _Compare> {
  public:
    typedef typename _Sequence::value_type value_type;

    explicit movable_priority_queue(const _Compare& __x, const _Sequence& __s) :
        std::priority_queue<_Tp, _Sequence, _Compare>(__x, __s) {}

    explicit movable_priority_queue(const _Compare& __x = _Compare(), _Sequence&& __s = _Sequence()) :
        std::priority_queue<_Tp, _Sequence, _Compare>(__x, std::move(__s)) {}

    using std::priority_queue<_Tp, _Sequence, _Compare>::empty;
    using std::priority_queue<_Tp, _Sequence, _Compare>::size;
    using std::priority_queue<_Tp, _Sequence, _Compare>::top;
    using std::priority_queue<_Tp, _Sequence, _Compare>::push;
    using std::priority_queue<_Tp, _Sequence, _Compare>::pop;
    using std::priority_queue<_Tp, _Sequence, _Compare>::emplace;
    using std::priority_queue<_Tp, _Sequence, _Compare>::swap;

    value_type pop_top() {
#ifdef __glibcxx_requires_nonempty
        __glibcxx_requires_nonempty();
#endif

        // arrange so that back contains desired
        std::pop_heap(this->c.begin(), this->c.end(), this->comp);
        value_type top = std::move(this->c.back());
        this->c.pop_back();
        return top;
    }
};

} // namespace arcticdb