// Copyright 2015 MongoDB Inc.
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

#include <type_traits>

#include <bsoncxx/stdx/optional.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

///
/// Class representing a view-or-value variant type.
///
template <typename View, typename Value>
class view_or_value {
   public:
    using view_type = View;
    using value_type = Value;

    ///
    /// Class View must be constructible from an instance of class Value.
    ///
    static_assert(std::is_constructible<View, Value>::value,
                  "View type must be constructible from a Value");

    ///
    /// Class View must be default constructible.
    ///
    static_assert(std::is_default_constructible<View>::value,
                  "View type must be default constructible");

    ///
    /// Default-constructs a view_or_value. This is equivalent to constructing a
    /// view_or_value with a default-constructed View.
    ///
    BSONCXX_INLINE view_or_value() = default;

    ///
    /// Construct a view_or_value from a View. When constructed with a View,
    /// this object is non-owning. The Value underneath the given View must outlive this object.
    ///
    /// @param view
    ///   A non-owning View.
    ///
    BSONCXX_INLINE view_or_value(View view) : _view{view} {}

    ///
    /// Constructs a view_or_value from a Value type. This object owns the passed-in Value.
    ///
    /// @param value
    ///   A Value type.
    ///
    BSONCXX_INLINE view_or_value(Value&& value) : _value(std::move(value)), _view(*_value) {}

    ///
    /// Construct a view_or_value from a copied view_or_value.
    ///
    BSONCXX_INLINE view_or_value(const view_or_value& other)
        : _value(other._value), _view(_value ? *_value : other._view) {}

    ///
    /// Assign to this view_or_value from a copied view_or_value.
    ///
    BSONCXX_INLINE view_or_value& operator=(const view_or_value& other) {
        _value = other._value;
        _view = _value ? *_value : other._view;
        return *this;
    }

    ///
    /// Construct a view_or_value from a moved-in view_or_value.
    ///

    /// TODO CXX-800: Create a noexcept expression to check the conditions that must be met.
    BSONCXX_INLINE view_or_value(view_or_value&& other) noexcept
        : _value{std::move(other._value)}, _view(_value ? *_value : std::move(other._view)) {
        other._view = View();
        other._value = stdx::nullopt;
    }

    ///
    /// Assign to this view_or_value from a moved-in view_or_value.
    ///
    /// TODO CXX-800: Create a noexcept expression to check the conditions that must be met.
    BSONCXX_INLINE view_or_value& operator=(view_or_value&& other) noexcept {
        _value = std::move(other._value);
        _view = _value ? *_value : std::move(other._view);
        other._view = View();
        other._value = stdx::nullopt;
        return *this;
    }

    ///
    /// Return whether or not this view_or_value owns an underlying Value.
    ///
    /// @return bool Whether we are owning.
    ///
    BSONCXX_INLINE bool is_owning() const noexcept {
        return static_cast<bool>(_value);
    }

    ///
    /// This type may be used as a View.
    ///
    /// @return a View into this view_or_value.
    ///
    BSONCXX_INLINE operator View() const {
        return _view;
    }

    ///
    /// Get a View for the type.
    ///
    /// @return a View into this view_or_value.
    ///
    BSONCXX_INLINE const View& view() const {
        return _view;
    }

   private:
    stdx::optional<Value> _value;
    View _view;
};

///
/// @{
///
/// Compare view_or_value objects for (in)-equality
///
/// @relates: view_or_value
///
template <typename View, typename Value>
BSONCXX_INLINE bool operator==(const view_or_value<View, Value>& lhs,
                               const view_or_value<View, Value>& rhs) {
    return lhs.view() == rhs.view();
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator!=(const view_or_value<View, Value>& lhs,
                               const view_or_value<View, Value>& rhs) {
    return !(lhs == rhs);
}
///
/// @}
///

///
/// @{
///
/// Mixed (in)-equality operators for view_or_value against View or Value types
///
/// @relates view_or_value
///
template <typename View, typename Value>
BSONCXX_INLINE bool operator==(const view_or_value<View, Value>& lhs, View rhs) {
    return lhs.view() == rhs;
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator==(View lhs, const view_or_value<View, Value>& rhs) {
    return rhs == lhs;
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator!=(const view_or_value<View, Value>& lhs, View rhs) {
    return !(lhs == rhs);
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator!=(View lhs, const view_or_value<View, Value>& rhs) {
    return !(rhs == lhs);
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator==(const view_or_value<View, Value>& lhs, const Value& rhs) {
    return lhs.view() == View(rhs);
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator==(const Value& lhs, const view_or_value<View, Value>& rhs) {
    return rhs == lhs;
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator!=(const view_or_value<View, Value>& lhs, const Value& rhs) {
    return !(lhs == rhs);
}

template <typename View, typename Value>
BSONCXX_INLINE bool operator!=(const Value& lhs, const view_or_value<View, Value>& rhs) {
    return !(rhs == lhs);
}
///
/// @}
///

BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
