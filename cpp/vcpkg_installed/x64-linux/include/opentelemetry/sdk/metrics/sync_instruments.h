// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/metrics/sync_instruments.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/state/metric_storage.h"
#include "opentelemetry/sdk_config.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

// forward declaration
class SyncWritableMetricStorage;

class Synchronous
{
public:
  Synchronous(InstrumentDescriptor instrument_descriptor,
              std::unique_ptr<SyncWritableMetricStorage> storage)
      : instrument_descriptor_(instrument_descriptor), storage_(std::move(storage))
  {}

protected:
  InstrumentDescriptor instrument_descriptor_;
  std::unique_ptr<SyncWritableMetricStorage> storage_;
};

template <typename T>
class LongCounter : public Synchronous, public opentelemetry::metrics::Counter<T>
{
public:
  LongCounter(InstrumentDescriptor instrument_descriptor,
              std::unique_ptr<SyncWritableMetricStorage> storage)
      : Synchronous(instrument_descriptor, std::move(storage))
  {
    if (!storage_)
    {
      OTEL_INTERNAL_LOG_ERROR("[LongCounter::LongCounter] - Error during constructing LongCounter."
                              << "The metric storage is invalid"
                              << "No value will be added");
    }
  }

  void Add(T value, const opentelemetry::common::KeyValueIterable &attributes) noexcept override
  {
    if (!storage_)
    {
      return;
    }
    auto context = opentelemetry::context::Context{};
    return storage_->RecordLong(value, attributes, context);
  }

  void Add(T value,
           const opentelemetry::common::KeyValueIterable &attributes,
           const opentelemetry::context::Context &context) noexcept override
  {
    if (!storage_)
    {
      return;
    }
    return storage_->RecordLong(value, attributes, context);
  }

  void Add(T value) noexcept override
  {
    auto context = opentelemetry::context::Context{};
    if (!storage_)
    {
      return;
    }
    return storage_->RecordLong(value, context);
  }

  void Add(T value, const opentelemetry::context::Context &context) noexcept override
  {
    if (!storage_)
    {
      return;
    }
    return storage_->RecordLong(value, context);
  }
};

class DoubleCounter : public Synchronous, public opentelemetry::metrics::Counter<double>
{

public:
  DoubleCounter(InstrumentDescriptor instrument_descriptor,
                std::unique_ptr<SyncWritableMetricStorage> storage);

  void Add(double value,
           const opentelemetry::common::KeyValueIterable &attributes) noexcept override;
  void Add(double value,
           const opentelemetry::common::KeyValueIterable &attributes,
           const opentelemetry::context::Context &context) noexcept override;

  void Add(double value) noexcept override;
  void Add(double value, const opentelemetry::context::Context &context) noexcept override;
};

class LongUpDownCounter : public Synchronous, public opentelemetry::metrics::UpDownCounter<int64_t>
{
public:
  LongUpDownCounter(InstrumentDescriptor instrument_descriptor,
                    std::unique_ptr<SyncWritableMetricStorage> storage);

  void Add(int64_t value,
           const opentelemetry::common::KeyValueIterable &attributes) noexcept override;
  void Add(int64_t value,
           const opentelemetry::common::KeyValueIterable &attributes,
           const opentelemetry::context::Context &context) noexcept override;

  void Add(int64_t value) noexcept override;
  void Add(int64_t value, const opentelemetry::context::Context &context) noexcept override;
};

class DoubleUpDownCounter : public Synchronous, public opentelemetry::metrics::UpDownCounter<double>
{
public:
  DoubleUpDownCounter(InstrumentDescriptor instrument_descriptor,
                      std::unique_ptr<SyncWritableMetricStorage> storage);

  void Add(double value,
           const opentelemetry::common::KeyValueIterable &attributes) noexcept override;
  void Add(double value,
           const opentelemetry::common::KeyValueIterable &attributes,
           const opentelemetry::context::Context &context) noexcept override;

  void Add(double value) noexcept override;
  void Add(double value, const opentelemetry::context::Context &context) noexcept override;
};

template <typename T>
class LongHistogram : public Synchronous, public opentelemetry::metrics::Histogram<T>
{
public:
  LongHistogram(InstrumentDescriptor instrument_descriptor,
                std::unique_ptr<SyncWritableMetricStorage> storage)
      : Synchronous(instrument_descriptor, std::move(storage))
  {
    if (!storage_)
    {
      OTEL_INTERNAL_LOG_ERROR(
          "[LongHistogram::LongHistogram] - Error during constructing LongHistogram."
          << "The metric storage is invalid"
          << "No value will be added");
    }
  }

  void Record(T value,
              const opentelemetry::common::KeyValueIterable &attributes,
              const opentelemetry::context::Context &context) noexcept override
  {
    if (value < 0)
    {
      OTEL_INTERNAL_LOG_WARN(
          "[LongHistogram::Record(value, attributes)] negative value provided to histogram Name:"
          << instrument_descriptor_.name_ << " Value:" << value);
      return;
    }
    return storage_->RecordLong(value, attributes, context);
  }

  void Record(T value, const opentelemetry::context::Context &context) noexcept override
  {
    if (value < 0)
    {
      OTEL_INTERNAL_LOG_WARN(
          "[LongHistogram::Record(value)] negative value provided to histogram Name:"
          << instrument_descriptor_.name_ << " Value:" << value);
      return;
    }
    return storage_->RecordLong(value, context);
  }
};

class DoubleHistogram : public Synchronous, public opentelemetry::metrics::Histogram<double>
{
public:
  DoubleHistogram(InstrumentDescriptor instrument_descriptor,
                  std::unique_ptr<SyncWritableMetricStorage> storage);

  void Record(double value,
              const opentelemetry::common::KeyValueIterable &attributes,
              const opentelemetry::context::Context &context) noexcept override;

  void Record(double value, const opentelemetry::context::Context &context) noexcept override;
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
