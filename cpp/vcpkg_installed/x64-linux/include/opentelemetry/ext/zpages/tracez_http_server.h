// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "nlohmann/json.hpp"
#include "opentelemetry/ext/zpages/static/tracez_index.h"
#include "opentelemetry/ext/zpages/static/tracez_script.h"
#include "opentelemetry/ext/zpages/static/tracez_style.h"
#include "opentelemetry/ext/zpages/tracez_data_aggregator.h"
#include "opentelemetry/ext/zpages/zpages_http_server.h"

#define HAVE_HTTP_DEBUG
#define HAVE_CONSOLE_LOG

using json = nlohmann::json;

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace zpages
{

class TracezHttpServer : public opentelemetry::ext::zpages::zPagesHttpServer
{
public:
  /**
   * Construct the server by initializing the endpoint for querying TraceZ aggregation data and
   * files, along with taking ownership of the aggregator whose data is used to send data to the
   * frontend
   * @param aggregator is the TraceZ Data Aggregator, which calculates aggregation info
   * @param host is the host where the TraceZ webpages will be displayed, default being localhost
   * @param port is the port where the TraceZ webpages will be displayed, default being 30000
   */
  TracezHttpServer(std::unique_ptr<opentelemetry::ext::zpages::TracezDataAggregator> &&aggregator,
                   const std::string &host = "localhost",
                   int port                = 30000)
      : opentelemetry::ext::zpages::zPagesHttpServer("/tracez", host, port),
        data_aggregator_(std::move(aggregator))
  {
    InitializeTracezEndpoint(*this);
  };

private:
  /**
   * Set the HTTP server to use the "Serve" callback to send the appropriate data when queried
   * @param server, which should be an instance of this object
   */
  void InitializeTracezEndpoint(TracezHttpServer &server) { server[endpoint_] = Serve; }

  /**
   * Updates the stored aggregation data (aggregations_) using the data aggregator
   */
  void UpdateAggregations();

  /**
   * First updates the stored aggregations, then translates that data from a C++ map to
   * a JSON object
   * @returns JSON object of collected spans bucket counts by name
   */
  json GetAggregations();

  /**
   * Using the stored aggregations, finds the span group with the right name and returns
   * its running span data as a JSON, only grabbing the fields needed for the frontend
   * @param name of the span group whose running data we want
   * @returns JSON representing running span data with the passed in name
   */
  json GetRunningSpansJSON(const std::string &name);

  /**
   * Using the stored aggregations, finds the span group with the right name and returns
   * its error span data as a JSON, only grabbing the fields needed for the frontend
   * @param name of the span group whose running data we want
   * @returns JSON representing eoor span data with the passed in name
   */
  json GetErrorSpansJSON(const std::string &name);

  /**
   * Using the stored aggregations, finds the span group with the right name and bucket index
   * returning its latency span data as a JSON, only grabbing the fields needed for the frontend
   * @param name of the span group whose latency data we want
   * @param index of which latency bucket to grab from
   * @returns JSON representing bucket span data with the passed in name and latency range
   */
  json GetLatencySpansJSON(const std::string &name, int latency_range_index);

  /**
   * Returns attributes, which have varied types, from a span data to convert into JSON
   * @param sample current span data, whose attributes we want to extract
   * @returns JSON representing attributes for a given threadsafe span data
   */
  json GetAttributesJSON(const opentelemetry::ext::zpages::ThreadsafeSpanData &sample);

  /**
   * Sets the response object with the TraceZ aggregation data based on the request endpoint
   * @param req is the HTTP request, which we use to figure out the response to send
   * @param resp is the HTTP response we want to send to the frontend, either webpage or TraceZ
   * aggregation data
   */
  HTTP_SERVER_NS::HttpRequestCallback Serve{
      [&](HTTP_SERVER_NS::HttpRequest const &req, HTTP_SERVER_NS::HttpResponse &resp) {
        std::string query = GetQuery(req.uri);  // tracez

        if (StartsWith(query, "get"))
        {
          resp.headers[HTTP_SERVER_NS::CONTENT_TYPE] = "application/json";
          query                                      = GetAfterSlash(query);
          if (StartsWith(query, "latency"))
          {
            auto queried_latency_name  = GetAfterSlash(query);
            auto queried_latency_index = std::stoi(GetBeforeSlash(queried_latency_name));
            auto queried_name          = GetAfterSlash(queried_latency_name);
            ReplaceHtmlChars(queried_name);
            resp.body = GetLatencySpansJSON(queried_name, queried_latency_index).dump();
          }
          else
          {
            auto queried_name = GetAfterSlash(query);
            ReplaceHtmlChars(queried_name);
            if (StartsWith(query, "aggregations"))
            {
              resp.body = GetAggregations().dump();
            }
            else if (StartsWith(query, "running"))
            {
              resp.body = GetRunningSpansJSON(queried_name).dump();
            }
            else if (StartsWith(query, "error"))
            {
              resp.body = GetErrorSpansJSON(queried_name).dump();
            }
            else
            {
              resp.body = json::array().dump();
            }
          }
        }
        else
        {
          if (StartsWith(query, "script.js"))
          {
            resp.headers[HTTP_SERVER_NS::CONTENT_TYPE] = "text/javascript";
            resp.body                                  = tracez_script;
          }
          else if (StartsWith(query, "style.css"))
          {
            resp.headers[HTTP_SERVER_NS::CONTENT_TYPE] = "text/css";
            resp.body                                  = tracez_style;
          }
          else if (query.empty() || query == "/tracez" || StartsWith(query, "index.html"))
          {
            resp.headers[HTTP_SERVER_NS::CONTENT_TYPE] = "text/html";
            resp.body                                  = tracez_index;
          }
          else
          {
            resp.headers[HTTP_SERVER_NS::CONTENT_TYPE] = "text/plain";
            resp.body                                  = "Invalid query: " + query;
          }
        }

        return 200;
      }};

  std::map<std::string, opentelemetry::ext::zpages::TracezData> aggregated_data_;
  std::unique_ptr<opentelemetry::ext::zpages::TracezDataAggregator> data_aggregator_;
};

}  // namespace zpages
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
