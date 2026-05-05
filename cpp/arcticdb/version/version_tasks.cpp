#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/version/version_core.hpp>

namespace arcticdb {
std::shared_ptr<pipelines::PipelineContext> SetupPipelineContextTask::operator()() {
    return version_store::setup_pipeline_context(store_, std::move(version_info_), *read_query_, read_options_);
}

IndexInformation::IndexInformation(
        std::pair<VariantKey, SegmentInMemory>&& index, std::optional<SegmentInMemory>&& column_stats
) :
    index_(std::move(index)),
    column_stats_(std::move(column_stats)) {}

} // namespace arcticdb
