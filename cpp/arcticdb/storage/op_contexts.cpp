/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/storage/op_contexts.hpp>

namespace arcticdb::storage::op_ctx {

template<> thread_local RemoveOpts OpContext<RemoveOpts>::active{};
template<> thread_local ReadKeyOpts OpContext<ReadKeyOpts>::active{};
template<> thread_local UpdateOpts OpContext<UpdateOpts>::active{};
}