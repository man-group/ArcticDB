#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/version/version_core.hpp>

namespace arcticdb {
std::tuple<std::shared_ptr<pipelines::PipelineContext>, VersionedItem> GetContextAndVersionedItemTask::operator()(
) const {
    auto pipeline_context = version_store::setup_pipeline_context(store_, version_info_, *read_query_, read_options_);
    auto res_versioned_item = version_store::generate_result_versioned_item(version_info_);

    return std::make_tuple(std::move(pipeline_context), std::move(res_versioned_item));
}
} // namespace arcticdb