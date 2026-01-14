/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/rapidcheck_generators.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>

RC_GTEST_PROP(ColumnStore, RapidCheck, (const std::map<std::string, TestDataFrame>& data_frames)) {
    using namespace arcticdb;

    auto store = std::make_shared<InMemoryStore>();
    std::vector<folly::Future<AtomKey>> futs;
    for (auto& data_frame : data_frames) {
        auto fut = write_test_frame(StringId(data_frame.first), data_frame.second, store).thenValue([](VariantKey key) {
            return to_atom(key);
        });
        futs.push_back(std::move(fut));
    }

    auto keys = folly::collectAll(futs.begin(), futs.end()).get();

    size_t count = 0;
    for (auto& data_frame : data_frames) {
        std::vector<std::string> errors;
        auto result = check_test_frame(data_frame.second, keys[count++].value(), store, errors);
        for (auto& err : errors)
            log::root().warn(err);
        RC_ASSERT(result);
    }
}
