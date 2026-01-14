/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations" // TODO experiment with not needing this, at the moment the
                                                           // non-deprecated version segfaults
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>
#include <rapidcheck/state.h>
#pragma GCC diagnostic pop