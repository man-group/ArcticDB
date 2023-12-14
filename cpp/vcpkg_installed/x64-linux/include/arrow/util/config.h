// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#define ARROW_VERSION_MAJOR 14
#define ARROW_VERSION_MINOR 0
#define ARROW_VERSION_PATCH 1
#define ARROW_VERSION ((ARROW_VERSION_MAJOR * 1000) + ARROW_VERSION_MINOR) * 1000 + ARROW_VERSION_PATCH

#define ARROW_VERSION_STRING "14.0.1"

#define ARROW_SO_VERSION "1400"
#define ARROW_FULL_SO_VERSION "1400.1.0"

#define ARROW_CXX_COMPILER_ID "GNU"
#define ARROW_CXX_COMPILER_VERSION "11.4.0"
#define ARROW_CXX_COMPILER_FLAGS "-fPIC -fdiagnostics-color=always"

#define ARROW_BUILD_TYPE "RELEASE"

#define ARROW_GIT_ID ""
#define ARROW_GIT_DESCRIPTION ""

#define ARROW_PACKAGE_KIND "vcpkg"

/* #undef ARROW_COMPUTE */
/* #undef ARROW_CSV */
/* #undef ARROW_CUDA */
/* #undef ARROW_DATASET */
/* #undef ARROW_FILESYSTEM */
/* #undef ARROW_FLIGHT */
/* #undef ARROW_FLIGHT_SQL */
#define ARROW_IPC
/* #undef ARROW_JEMALLOC */
/* #undef ARROW_JEMALLOC_VENDORED */
/* #undef ARROW_JSON */
/* #undef ARROW_ORC */
/* #undef ARROW_PARQUET */
/* #undef ARROW_SUBSTRAIT */

#define ARROW_ENABLE_THREADING
/* #undef ARROW_GCS */
/* #undef ARROW_S3 */
#define ARROW_USE_NATIVE_INT128
/* #undef ARROW_WITH_MUSL */
/* #undef ARROW_WITH_OPENTELEMETRY */
/* #undef ARROW_WITH_UCX */
/* #undef PARQUET_REQUIRE_ENCRYPTION */
