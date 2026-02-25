#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/version/version_core.hpp>

namespace arcticdb {
std::shared_ptr<pipelines::PipelineContext> SetupPipelineContextTask::operator()() {
    return version_store::setup_pipeline_context(
            store_, version_info_, *read_query_, read_options_, std::move(index_information_)
    );
}
} // namespace arcticdb