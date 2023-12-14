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

#include <bsoncxx/document/element.hpp>
#include <bsoncxx/stdx/string_view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace document {

///
/// A read-only, non-owning view of a BSON document.
///
class BSONCXX_API view {
   public:
    class BSONCXX_API const_iterator;
    using iterator = const_iterator;

    ///
    /// Default constructs a view. The resulting view will be initialized to point at
    /// an empty BSON document.
    ///
    view();

    ///
    /// Constructs a view from a buffer. The caller is responsible for ensuring that
    /// the lifetime of the resulting view is a subset of the buffer's.
    ///
    /// @param data
    ///   A buffer containing a valid BSON document.
    /// @param length
    ///   The size of the buffer, in bytes.
    ///
    view(const std::uint8_t* data, std::size_t length);

    ///
    /// @returns A const_iterator to the first element of the document.
    ///
    const_iterator cbegin() const;

    ///
    /// @returns A const_iterator to the past-the-end element of the document.
    ///
    const_iterator cend() const;

    ///
    /// @returns A const_iterator to the first element of the document.
    ///
    const_iterator begin() const;

    ///
    /// @returns A const_iterator to the past-the-end element of the document.
    ///
    const_iterator end() const;

    ///
    /// Finds the first element of the document with the provided key. If there is
    /// no such element, the past-the-end iterator will be returned. The runtime of
    /// find() is linear in the length of the document. This method only searches
    /// the top-level document, and will not recurse to any subdocuments.
    ///
    /// @remark In BSON, keys are not required to be unique. If there are multiple
    /// elements with a matching key in the document, the first matching element from
    /// the start will be returned.
    ///
    /// @param key
    ///   The key to search for.
    ///
    /// @return An iterator to the matching element, if found, or the past-the-end iterator.
    ///
    const_iterator find(stdx::string_view key) const;

    ///
    /// Finds the first element of the document with the provided key. If there is no
    /// such element, the invalid document::element will be returned. The runtime of operator[]
    /// is linear in the length of the document.
    ///
    /// @param key
    ///   The key to search for.
    ///
    /// @return The matching element, if found, or the invalid element.
    ///
    element operator[](stdx::string_view key) const;

    ///
    /// Access the raw bytes of the underlying document.
    ///
    /// @return A (non-owning) pointer to the view's buffer.
    ///
    const std::uint8_t* data() const;

    ///
    /// Gets the length of the underlying buffer.
    ///
    /// @remark This is not the number of elements in the document.
    /// To compute the number of elements, use std::distance.
    ///
    /// @return The length of the document, in bytes.
    ///
    std::size_t length() const;

    ///
    /// Checks if the underlying document is empty, i.e. it is equivalent to
    /// the trivial document '{}'.
    ///
    /// @return true if the underlying document is empty.
    ///
    bool empty() const;

    ///
    /// @{
    ///
    /// Compare two document views for (in)-equality
    ///
    /// @relates view
    ///
    friend BSONCXX_API bool BSONCXX_CALL operator==(view, view);
    friend BSONCXX_API bool BSONCXX_CALL operator!=(view, view);
    ///
    /// @}
    ///

   private:
    const std::uint8_t* _data;
    std::size_t _length;
};

///
/// A const iterator over the contents of a document view.
///
/// This iterator type provides a const forward iterator interface to document
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
    /// Compares two const_iterators for (in)-equality
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

}  // namespace document
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
