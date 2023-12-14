// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/type_traits.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace metrics
{

class SynchronousInstrument
{
public:
  SynchronousInstrument()          = default;
  virtual ~SynchronousInstrument() = default;
};

template <class T>
class Counter : public SynchronousInstrument
{

public:
  /**
   * Add adds the value to the counter's sum
   *
   * @param value The increment amount. MUST be non-negative.
   */
  virtual void Add(T value) noexcept = 0;

  virtual void Add(T value, const context::Context &context) noexcept = 0;

  /**
   * Add adds the value to the counter's sum. The attributes should contain
   * the keys and values to be associated with this value.  Counters only
   * accept positive valued updates.
   *
   * @param value The increment amount. MUST be non-negative.
   * @param attributes the set of attributes, as key-value pairs
   */

  virtual void Add(T value, const common::KeyValueIterable &attributes) noexcept = 0;

  virtual void Add(T value,
                   const common::KeyValueIterable &attributes,
                   const context::Context &context) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes) noexcept
  {
    auto context = context::Context{};
    this->Add(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes, const context::Context &context) noexcept
  {
    this->Add(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
               attributes) noexcept
  {
    auto context = context::Context{};
    this->Add(value,
              nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                  attributes.begin(), attributes.end()},
              context);
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>> attributes,
           const context::Context &context) noexcept
  {
    this->Add(value,
              nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                  attributes.begin(), attributes.end()},
              context);
  }
};

/** A histogram instrument that records values. */

template <class T>
class Histogram : public SynchronousInstrument
{
public:
  /**
   * Records a value.
   *
   * @param value The measurement value. MUST be non-negative.
   */
  virtual void Record(T value, const context::Context &context) noexcept = 0;

  /**
   * Records a value with a set of attributes.
   *
   * @param value The measurement value. MUST be non-negative.
   * @param attributes A set of attributes to associate with the count.
   */
  virtual void Record(T value,
                      const common::KeyValueIterable &attributes,
                      const context::Context &context) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Record(T value, const U &attributes, const context::Context &context) noexcept
  {
    this->Record(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  void Record(
      T value,
      std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>> attributes,
      const context::Context &context) noexcept
  {
    this->Record(value,
                 nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                     attributes.begin(), attributes.end()},
                 context);
  }
};

/** An up-down-counter instrument that adds or reduce values. */

template <class T>
class UpDownCounter : public SynchronousInstrument
{
public:
  /**
   * Adds a value.
   *
   * @param value The amount of the measurement.
   */
  virtual void Add(T value) noexcept = 0;

  virtual void Add(T value, const context::Context &context) noexcept = 0;

  /**
   * Add a value with a set of attributes.
   *
   * @param value The increment amount. May be positive, negative or zero.
   * @param attributes A set of attributes to associate with the count.
   */
  virtual void Add(T value, const common::KeyValueIterable &attributes) noexcept = 0;

  virtual void Add(T value,
                   const common::KeyValueIterable &attributes,
                   const context::Context &context) noexcept = 0;

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes) noexcept
  {
    auto context = context::Context{};
    this->Add(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  template <class U,
            nostd::enable_if_t<common::detail::is_key_value_iterable<U>::value> * = nullptr>
  void Add(T value, const U &attributes, const context::Context &context) noexcept
  {
    this->Add(value, common::KeyValueIterableView<U>{attributes}, context);
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>>
               attributes) noexcept
  {
    auto context = context::Context{};
    this->Add(value,
              nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                  attributes.begin(), attributes.end()},
              context);
  }

  void Add(T value,
           std::initializer_list<std::pair<nostd::string_view, common::AttributeValue>> attributes,
           const context::Context &context) noexcept
  {
    this->Add(value,
              nostd::span<const std::pair<nostd::string_view, common::AttributeValue>>{
                  attributes.begin(), attributes.end()},
              context);
  }
};

}  // namespace metrics
OPENTELEMETRY_END_NAMESPACE
