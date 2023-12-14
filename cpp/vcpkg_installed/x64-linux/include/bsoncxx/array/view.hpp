// Copyright 2014 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>

#include <bsoncxx/array/element.hpp>
#include <bsoncxx/document/view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

namespace types {
namespace bson_value {
class view;
}  // namespace bson_value
}  // namespace types

namespace array {

///
/// A read-only, non-owning view of a BSON document.
///
class BSONCXX_API view {
   public:
    class BSONCXX_API const_iterator;
    using iterator = const_iterator;

    ///
    /// @returns A const_iterator to the first element of the array.
    ///
    const_iterator cbegin() const;

    ///
    /// @returns A const_iterator to the past-the-end element of the array.
    ///
    const_iterator cend() const;

    ///
    /// @returns A const_iterator to the first element of the array.
    ///
    const_iterator begin() const;

    ///
    /// @returns A const_iterator to the past-the-end element of the array.
    ///
    const_iterator end() const;

    ///
    /// Indexes into this BSON array. If the index is out-of-bounds, a past-the-end iterator
    /// will be returned. As BSON represents arrays as documents, the runtime of find() is
    /// linear in the length of the array.
    ///
    /// @param i
    ///   The index of the element.
    ///
    /// @return An iterator to the element if it exists, or the past-the-end iterator.
    ///
    const_iterator find(std::uint32_t i) const;

    ///
    /// Indexes into this BSON array. If the index is out-of-bounds, the invalid array::element
    /// will be returned. As BSON represents arrays as documents, the runtime of operator[] is
    /// linear in the length of the array.
    ///
    /// @param i
    ///   The index of the element.
    ///
    /// @return The element if it exists, or the invalid element.
    ///
    element operator[](std::uint32_t i) const;

    ///
    /// Default constructs a view. The resulting view will be initialized to point at
    /// an empty BSON array.
    ///
    view();

    ///
    /// Constructs a view from a buffer. The caller is responsible for ensuring that
    /// the lifetime of the resulting view is a subset of the buffer's.
    ///
    /// @param data
    ///   A buffer containing a valid BSON array.
    /// @param length
    ///   The size of the buffer, in bytes.
    ///
    view(const std::uint8_t* data, std::size_t length);

    ///
    /// Access the raw bytes of the underlying array.
    ///
    /// @return A (non-owning) pointer to the view's buffer.
    ///
    const std::uint8_t* data() const;

    ///
    /// Gets the length of the underlying buffer.
    ///
    /// @remark This is not the number of elements in the array.
    /// To compute the number of elements, use std::distance.
    ///
    /// @return The length of the array, in bytes.
    ///
    std::size_t length() const;

    ///
    /// Checks if the underlying buffer is empty, i.e. it is equivalent to
    /// the trivial array '[]'.
    ///
    /// @return true if the underlying document is empty.
    ///
    bool empty() const;

    ///
    /// Conversion operator unwrapping a document::view
    ///
    operator document::view() const;

    ///
    /// @{
    ///
    /// Compare two views for (in)-equality
    ///
    /// @relates view
    ///
    friend BSONCXX_API bool BSONCXX_CALL operator==(view, view);
    friend BSONCXX_API bool BSONCXX_CALL operator!=(view, view);
    ///
    /// @}
    ///

   private:
    document::view _view;
};

///
/// A const iterator over the contents of an array view.
///
/// This iterator type provides a const forward iterator interface to array
/// view elements.
///
class BSONCXX_API view::const_iterator {
   public:
    ///
    /// std::iterator_traits
    ///
    using value_type = element;
    using reference = element&;
    using pointer = element*;
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;

    const_iterator();
    explicit const_iterator(const element& element);

    reference operator*();
    pointer operator->();

    const_iterator& operator++();
    const_iterator operator++(int);

    ///
    /// @{
    ///
    /// Compare two const_iterators for (in)-equality
    ///
    /// @relates view::const_iterator
    ///
    friend BSONCXX_API bool BSONCXX_CALL operator==(const const_iterator&, const const_iterator&);
    friend BSONCXX_API bool BSONCXX_CALL operator!=(const const_iterator&, const const_iterator&);
    ///
    /// @}
    ///

   private:
    element _element;
};

}  // namespace array
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
