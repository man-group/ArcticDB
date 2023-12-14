// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <mutex>
#include <vector>
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/meter_provider.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/metrics/meter.h"
#include "opentelemetry/sdk/metrics/meter_context.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

/*
  MAINTAINER:

  The best design is to return an API object:
    std::unique_ptr<opentelemetry::metrics::MeterProvider>
  to shield the calling application from SDK implementation details.

  This however assumes that the SDK object can be created in one call,
  instead of making multiple calls to the SDK to setup a meter provider.

  Because existing code, already advertised in examples:
  - creates an instance of sdk::MeterProvider
  - calls SDK methods on it to continue the setup, such as
    MeterProvider::AddMetricReader()
    MeterProvider::AddView()
  existing applications will need to access the underlying
  class sdk::MeterProvider.

  We need to decide whether to return:
  - (1) std::unique_ptr<opentelemetry::metrics::MeterProvider>
  - (2) std::unique_ptr<opentelemetry::metrics::sdk::MeterProvider>
  from a Create() method.

  In the long term, (1) is better, but forces users to use a down cast,
  to make additional calls to the SDK class, until such a time when
  the builders can take all the necessary input at once,
  for example using a std::vector<MetricReader> to add all readers.

  Implementing (2) is forcing technical debt, and prevents the
  calling application configuring the SDK to be decoupled from it,
  making deployment of shared libraries much more difficult.

  The design choice here is to return (1) an API MeterProvider,
  even if this forces, temporarily, existing applications to use a downcast.
*/

class MeterProviderFactory
{
public:
  static std::unique_ptr<opentelemetry::metrics::MeterProvider> Create();

  static std::unique_ptr<opentelemetry::metrics::MeterProvider> Create(
      std::unique_ptr<ViewRegistry> views);

  static std::unique_ptr<opentelemetry::metrics::MeterProvider> Create(
      std::unique_ptr<ViewRegistry> views,
      const opentelemetry::sdk::resource::Resource &resource);

  static std::unique_ptr<opentelemetry::metrics::MeterProvider> Create(
      std::unique_ptr<sdk::metrics::MeterContext> context);
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
