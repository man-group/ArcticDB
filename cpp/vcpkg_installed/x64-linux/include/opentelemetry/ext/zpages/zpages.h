// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>

#include "opentelemetry/ext/zpages/tracez_data_aggregator.h"
#include "opentelemetry/ext/zpages/tracez_http_server.h"
#include "opentelemetry/ext/zpages/tracez_processor.h"
#include "opentelemetry/ext/zpages/tracez_shared_data.h"

#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/provider.h"

using opentelemetry::ext::zpages::TracezDataAggregator;
using opentelemetry::ext::zpages::TracezHttpServer;
using opentelemetry::ext::zpages::TracezSharedData;
using opentelemetry::ext::zpages::TracezSpanProcessor;
using std::chrono::microseconds;

/**
 * Wrapper for zPages that initializes all the components required for zPages,
 * and starts the HTTP server in the constructor and ends it in the destructor.
 * The constructor and destructor for this object is private to prevent
 * creation other than by calling the static function Initialize(). This follows the
 * meyers singleton pattern and only a single instance of the class is allowed.
 */
class ZPages
{
public:
  /**
   * This function is called if the user wishes to include zPages in their
   * application. It creates a static instance of this class and replaces the
   * global TracerProvider with one that delegates spans to tracez.
   */
  static void Initialize() { Instance().ReplaceGlobalProvider(); }

  /**
   * Returns the singletone instnace of ZPages, useful for attaching z-pages span processors to
   * non-global providers.
   *
   * Note: This will instantiate the Tracez instance and webserver if it hasn't already been
   * instantiated.
   */
  static ZPages &Instance()
  {
    static ZPages instance;
    return instance;
  }

  /** Replaces the global tracer provider with an instance that exports to tracez. */
  void ReplaceGlobalProvider()
  {
    // GCC 4.8 can't infer the type coercion.
    std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> processor(
        MakeSpanProcessor().release());
    auto tracez_provider_ = opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
        new opentelemetry::sdk::trace::TracerProvider(std::move(processor)));
    opentelemetry::trace::Provider::SetTracerProvider(tracez_provider_);
  }

  /** Retruns a new span processor that will output to z-pages. */
  std::unique_ptr<TracezSpanProcessor> MakeSpanProcessor()
  {
    return std::unique_ptr<TracezSpanProcessor>(new TracezSpanProcessor(tracez_shared_));
  }

private:
  /**
   * Constructor is responsible for initializing the tracer, tracez processor,
   * tracez data aggregator and the tracez server. The server is also started in
   * constructor.
   */
  ZPages()
  {
    // Construct shared data nd start tracez webserver.
    tracez_shared_ = std::make_shared<TracezSharedData>();
    auto tracez_aggregator =
        std::unique_ptr<TracezDataAggregator>(new TracezDataAggregator(tracez_shared_));
    tracez_server_ =
        std::unique_ptr<TracezHttpServer>(new TracezHttpServer(std::move(tracez_aggregator)));
    tracez_server_->start();
  }

  ~ZPages()
  {
    // shut down the server when the object goes out of scope(at the end of the
    // program)
    tracez_server_->stop();
  }
  std::shared_ptr<TracezSharedData> tracez_shared_;
  std::unique_ptr<TracezHttpServer> tracez_server_;
};
