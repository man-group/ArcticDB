/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/python_bindings.hpp>
#include <arcticdb/codec/python_bindings.hpp>
#include <arcticdb/column_store/python_bindings.hpp>
#include <arcticdb/storage/python_bindings.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/stream/python_bindings.hpp>
#include <arcticdb/toolbox/python_bindings.hpp>
#include <arcticdb/version/python_bindings.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/trace.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/arctic_version.hpp>
#include <arcticdb/python/python_handler_data.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/global_lifetimes.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/error_code.hpp>
#include <arcticdb/util/type_handler.hpp>
#include <arcticdb/python/python_handlers.hpp>
#include <arcticdb/util/pybind_mutex.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <util/gil_safe_py_none.hpp>

#include <pybind11/pybind11.h>
#include <mongocxx/exception/logic_error.hpp>

#include <logger.pb.h>

namespace py = pybind11;

enum class LoggerId {
    ROOT,
    STORAGE,
    IN_MEM,
    CODEC,
    VERSION,
    MEMORY,
    TIMINGS,
    LOCK,
    SCHEDULE,
    SYMBOL,
    SNAPSHOT
};

void register_log(py::module && log) {
    log.def("configure", [](const py::object & py_log_conf, bool force=false){
        arcticdb::proto::logger::LoggersConfig config;
        arcticdb::python_util::pb_from_python(py_log_conf, config);
        return arcticdb::log::Loggers::instance().configure(config, force);
    }, py::arg("py_log_conf"), py::arg("force")=false);

     py::enum_<spdlog::level::level_enum>(log, "LogLevel")
             .value("DEBUG", spdlog::level::level_enum::debug)
             .value("INFO", spdlog::level::level_enum::info)
             .value("WARN", spdlog::level::level_enum::warn)
             .value("ERROR", spdlog::level::level_enum::err)
             .export_values()
     ;
     py::enum_<LoggerId>(log, "LoggerId")
             .value("ROOT", LoggerId::ROOT)
             .value("STORAGE", LoggerId::STORAGE)
             .value("IN_MEM", LoggerId::IN_MEM)
             .value("CODEC", LoggerId::CODEC)
             .value("VERSION", LoggerId::VERSION)
             .value("MEMORY", LoggerId::MEMORY)
             .value("TIMINGS", LoggerId::TIMINGS)
             .value("LOCK", LoggerId::LOCK)
             .value("SCHEDULE", LoggerId::SCHEDULE)
             .value("SYMBOL", LoggerId::SYMBOL)
             .value("SNAPSHOT", LoggerId::SNAPSHOT)
             .export_values()
    ;
    auto choose_logger = [&](LoggerId log_id) -> decltype(arcticdb::log::storage()) /* logger ref */{
        switch (log_id) {
            case LoggerId::STORAGE:
                return arcticdb::log::storage();
            case LoggerId::IN_MEM:
                return arcticdb::log::inmem();
            case LoggerId::CODEC:
                return arcticdb::log::codec();
            case LoggerId::MEMORY:
                return arcticdb::log::memory();
            case LoggerId::VERSION:
                return arcticdb::log::version();
            case LoggerId::ROOT:
                return arcticdb::log::root();
            case LoggerId::TIMINGS:
                return arcticdb::log::timings();
            case LoggerId::LOCK:
                return arcticdb::log::lock();
            case LoggerId::SCHEDULE:
                return arcticdb::log::schedule();
            case LoggerId::SYMBOL:
                return arcticdb::log::symbol();
            case LoggerId::SNAPSHOT:
                return arcticdb::log::snapshot();
            default:
                arcticdb::util::raise_rte("Unsupported logger id");
        }
    };

    log.def("log",[&](LoggerId log_id, spdlog::level::level_enum level, const std::string & msg){
        //assuming formatting done in python
        auto & logger = choose_logger(log_id);
        switch(level){
            case spdlog::level::level_enum::debug:
                logger.debug(msg);
                break;
            case spdlog::level::level_enum::info:
                logger.info(msg);
                break;
            case spdlog::level::level_enum::warn:
                logger.warn(msg);
                break;
            case spdlog::level::level_enum::err:
                logger.error(msg);
                break;
            default:
                arcticdb::util::raise_rte("Unsupported log level", spdlog::level::to_string_view(level));
        }
    });

    log.def("is_active", [&](LoggerId log_id, spdlog::level::level_enum level){
       auto & logger = choose_logger(log_id);
       return logger.should_log(level);
    });

    log.def("flush_all", [](){
        arcticdb::log::Loggers::instance().flush_all();
    });
}

void register_configs_map_api(py::module& m) {
    using namespace arcticdb;
#define EXPOSE_TYPE(LABEL, TYPE) \
    m.def("get_config_" #LABEL, [](const std::string& label) { return ConfigsMap::instance()->get_##LABEL(label); }); \
    m.def("set_config_" #LABEL, [](const std::string& label, TYPE value)  { ConfigsMap::instance()->set_##LABEL(label, value); }); \
    m.def("unset_config_" #LABEL, [](const std::string& label)  { ConfigsMap::instance()->unset_##LABEL(label); });

    EXPOSE_TYPE(int, int64_t)
    EXPOSE_TYPE(string, std::string)
    EXPOSE_TYPE(double, double)
#undef EXPOSE_TYPE
}

#ifdef WIN32
__declspec(noinline)
#else
__attribute__((noinline))
#endif
int rec_call(int i){
    if(i < 0){
        throw std::invalid_argument("Explosion");
    } else if(i == 0) return 7;
    if(i % 3 == 0)
        return rec_call(i - 4);
    else
        return rec_call(i-1);
}

void register_termination_handler() {
    std::set_terminate([]{
        auto eptr = std::current_exception();
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception &e) {
            arcticdb::log::root().error("Terminate called in thread {}: {}\n Aborting", arcticdb::get_thread_id(), e.what());
            std::abort();
        }
    });
}

void register_error_code_ecosystem(py::module& m, py::exception<arcticdb::ArcticException>& base_exception) {
    using namespace arcticdb;

    auto cat_enum = py::enum_<ErrorCategory>(m, "ErrorCategory");
    for (const auto& [member, name]: get_error_category_names()) {
        cat_enum.value(name, member);
    }

    auto code_enum = py::enum_<ErrorCode>(m, "ErrorCode");
    py::dict enum_value_to_prefix{};
    for (auto code : get_error_codes()) {
        auto data = get_error_code_data(code);
        code_enum.value(data.name_.data(), code, data.as_string_.data());
        enum_value_to_prefix[py::int_((int) code)] = data.as_string_;
    }

    setattr(m, "enum_value_to_prefix", enum_value_to_prefix);
    m.def("get_error_category", &get_error_category);

    // legacy exception base type kept for backwards compat with Man Python client
    struct ArcticCompatibilityException : public ArcticException {};
    auto compat_exception = py::register_exception<ArcticCompatibilityException>(
            m, "_ArcticLegacyCompatibilityException", base_exception);

    static py::exception<InternalException> internal_exception(m, "InternalException", compat_exception.ptr());
    static py::exception<StorageException> storage_exception(m, "StorageException", compat_exception.ptr());
    static py::exception<LMDBMapFullException> lmdb_map_full_exception(m, "LmdbMapFullError", storage_exception.ptr());
    static py::exception<UserInputException> user_input_exception(m, "UserInputException", compat_exception.ptr());

    py::register_exception_translator([](std::exception_ptr p) {
        try {
            if (p) std::rethrow_exception(p);
        } catch (const mongocxx::v_noabi::logic_error& e){
            py::set_error(user_input_exception, e.what());
        } catch (const UserInputException& e){
            py::set_error(user_input_exception, e.what());
        } catch (const arcticdb::InternalException& e){
            py::set_error(internal_exception, e.what());
        } catch (const LMDBMapFullException& e) {
            std::string msg = fmt::format("E5003: LMDB map is full. Close and reopen your LMDB backed Arctic instance with a "
                                          "larger map size. For example to open `/tmp/a/b/` with a map size of 5GB, "
                                          "use `Arctic(\"lmdb:///tmp/a/b?map_size=5GB\")`. Also see the "
                                          "[LMDB documentation](http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5). "
                                          "LMDB info: message=[{}]", e.what());
            py::set_error(lmdb_map_full_exception, msg.c_str());
        } catch (const StorageException& e) {
            py::set_error(storage_exception, e.what());
        } catch (const py::stop_iteration &e){
            // let stop iteration bubble up, since this is how python implements iteration termination
            std::rethrow_exception(p);
        } catch (const std::exception &e) {
            std::string msg = fmt::format("{}({})", arcticdb::get_type_name(typeid(e)), e.what());
            py::set_error(internal_exception, msg.c_str());
        }
    });

    py::register_exception<storage::DuplicateKeyException>(m, "DuplicateKeyException", storage_exception.ptr());
    py::register_exception<PermissionException>(m, "PermissionException", storage_exception.ptr());

    py::register_exception<SchemaException>(m, "SchemaException", compat_exception.ptr());
    py::register_exception<NormalizationException>(m, "NormalizationException", compat_exception.ptr());
    py::register_exception<MissingDataException>(m, "MissingDataException", compat_exception.ptr());

    auto sorting_exception = py::register_exception<SortingException>(m, "SortingException", compat_exception.ptr());
    py::register_exception<UnsortedDataException>(m, "UnsortedDataException", sorting_exception.ptr());
    py::register_exception<CompatibilityException>(m, "CompatibilityException", compat_exception.ptr());
    py::register_exception<CodecException>(m, "CodecException", compat_exception.ptr());
}

void reinit_scheduler() {
    ARCTICDB_DEBUG(arcticdb::log::version(), "Post-fork, reinitializing the task scheduler");
    arcticdb::async::TaskScheduler::reattach_instance();
}

void reinit_lmdb_warning() {
    ARCTICDB_DEBUG(arcticdb::log::version(), "Post-fork in child, resetting LMDB warning counter");
    arcticdb::storage::lmdb::LmdbStorage::reset_warning_counter();
}

void register_instrumentation(py::module && m){
    auto remotery = m.def_submodule("remotery");
#if defined(USE_REMOTERY)
    py::class_<RemoteryInstance, std::shared_ptr<RemoteryInstance>>(remotery, "Instance");
    remotery.def("configure", [](const py::object & py_config){
        arcticdb::proto::utils::RemoteryConfig config;
        arcticdb::python_util::pb_from_python(py_config, config);
        RemoteryConfigInstance::instance()->config.CopyFrom(config);
    });
    remotery.def("log", [](std::string s ARCTICDB_UNUSED){
       ARCTICDB_SAMPLE_LOG(s.c_str())
    });
#endif
}

void register_metrics(py::module && m){

    auto prometheus = m.def_submodule("prometheus");
    py::class_<arcticdb::PrometheusInstance, std::shared_ptr<arcticdb::PrometheusInstance>>(prometheus, "Instance");

    py::class_<arcticdb::MetricsConfig, std::shared_ptr<arcticdb::MetricsConfig>>(prometheus, "MetricsConfig")
    .def(py::init<const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, const arcticdb::MetricsConfig::Model>());

    py::enum_<arcticdb::MetricsConfig::Model>(prometheus, "MetricsConfigModel")
            .value("NO_INIT", arcticdb::MetricsConfig::Model::NO_INIT)
            .value("PUSH", arcticdb::MetricsConfig::Model::PUSH)
            .value("PULL", arcticdb::MetricsConfig::Model::PULL)
            .export_values()
    ;
}

/// Register handling of non-trivial types. For more information @see arcticdb::TypeHandlerRegistry and
/// @see arcticdb::ITypeHandler
void register_type_handlers() {
    using namespace arcticdb;
    TypeHandlerRegistry::instance()->register_handler(make_scalar_type(DataType::EMPTYVAL), arcticdb::EmptyHandler());
    TypeHandlerRegistry::instance()->register_handler(make_scalar_type(DataType::BOOL_OBJECT8),  arcticdb::BoolHandler());

    register_array_types();
    register_string_types();

    register_python_handler_data_factory();
}

PYBIND11_MODULE(arcticdb_ext, m) {
    m.doc() = R"pbdoc(
        ArcticDB Extension plugin

        Top level package of ArcticDB extension plugin.
    )pbdoc";
    auto programName ="__arcticdb_logger__";
    google::InitGoogleLogging(programName);
    using namespace arcticdb;
    GilSafePyNone::instance(); // Ensure that the GIL is held when the static py::none gets allocated
#ifndef WIN32
    // No fork() in Windows, so no need to register the handler
    pthread_atfork(nullptr, nullptr, &SingleThreadMutexHolder::reset_mutex);
    pthread_atfork(nullptr, nullptr, &reinit_scheduler);
    pthread_atfork(nullptr, nullptr, &reinit_lmdb_warning);
    pthread_atfork(nullptr, nullptr, &register_python_handler_data_factory);
#endif
    // Set up the global exception handlers first, so module-specific exception handler can override it:
    auto exceptions = m.def_submodule("exceptions");
    auto base_exception = py::register_exception<arcticdb::ArcticException>(
            exceptions, "ArcticException", PyExc_RuntimeError);
    register_error_code_ecosystem(exceptions, base_exception);

    arcticdb::async::register_bindings(m);
    arcticdb::codec::register_bindings(m);
    arcticdb::column_store::register_bindings(m);

    auto storage_submodule = m.def_submodule("storage", "Segment storage implementation apis");
    auto no_data_found_exception = py::register_exception<arcticdb::storage::NoDataFoundException>(
            storage_submodule, "NoDataFoundException", base_exception.ptr());
    arcticdb::storage::apy::register_bindings(storage_submodule, base_exception);

    arcticdb::stream::register_bindings(m);
    arcticdb::toolbox::apy::register_bindings(m, base_exception);

    m.def("get_version_string", &arcticdb::get_arcticdb_version_string);

    auto version_submodule = m.def_submodule("version_store", "Versioned storage implementation apis");
    arcticdb::version_store::register_bindings(version_submodule, base_exception);
    py::register_exception<arcticdb::NoSuchVersionException>(
            version_submodule, "NoSuchVersionException", no_data_found_exception.ptr());

    register_configs_map_api(m);
    register_log(m.def_submodule("log"));
    register_instrumentation(m.def_submodule("instrumentation"));
    register_metrics(m.def_submodule("metrics"));
    register_type_handlers();

    auto cleanup_callback = []() {
        using namespace arcticdb;
        ARCTICDB_DEBUG(log::version(), "Running cleanup callback");
        shutdown_globals();
    };

    m.add_object("_cleanup", py::capsule(cleanup_callback));

    register_termination_handler();

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "dev";
#endif
}
