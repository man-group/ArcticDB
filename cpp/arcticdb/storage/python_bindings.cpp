/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/storage/python_bindings.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <pybind11/stl.h>

#include <arcticdb/util/variant.hpp>

#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_manager.hpp>
#include <arcticdb/storage/protobuf_mappings.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/storage/config_resolvers.hpp>

namespace py = pybind11;

namespace arcticdb::storage::apy {

using namespace python_util;

std::shared_ptr<LibraryIndex> create_library_index(const std::string &environment_name, const py::object &py_envs) {
    arcticdb::proto::storage::EnvironmentConfigsMap envs;
    pb_from_python(py_envs, envs);
    auto env_by_id = convert_environment_config(envs);
    auto mem_resolver = create_in_memory_resolver(env_by_id);
    return std::make_shared<LibraryIndex>(EnvironmentName{environment_name}, mem_resolver);
}

void register_bindings(py::module &m) {
    auto storage = m.def_submodule("storage", "Segment storage implementation apis");

    py::enum_<KeyType>(storage, "KeyType")
        .value("STREAM_GROUP", KeyType::STREAM_GROUP)
        .value("VERSION", KeyType::VERSION)
        .value("VERSION_JOURNAL", KeyType::VERSION_JOURNAL)
        .value("GENERATION", KeyType::GENERATION)
        .value("TABLE_DATA", KeyType::TABLE_DATA)
        .value("TABLE_INDEX", KeyType::TABLE_INDEX)
        .value("METRICS", KeyType::METRICS)
        .value("SNAPSHOT", KeyType::SNAPSHOT)
        .value("SYMBOL_LIST", KeyType::SYMBOL_LIST)
        .value("VERSION_REF", KeyType::VERSION_REF)
        .value("STORAGE_INFO", KeyType::STORAGE_INFO)
        .value("APPEND_REF", KeyType::APPEND_REF)
        .value("LOCK", KeyType::LOCK)
        .value("SNAPSHOT_REF", KeyType::SNAPSHOT_REF)
        .value("TOMBSTONE", KeyType::TOMBSTONE)
        .value("APPEND_DATA", KeyType::APPEND_DATA)
        .value("MULTI_KEY", KeyType::MULTI_KEY)
        .value("LOG", KeyType::LOG)
        .value("PARTITION", KeyType::PARTITION)
        .value("OFFSET", KeyType::OFFSET)
        .value("BACKUP_SNAPSHOT_REF", KeyType::BACKUP_SNAPSHOT_REF)
        .value("TOMBSTONE_ALL", KeyType::TOMBSTONE_ALL)
        .value("SNAPSHOT_TOMBSTONE", KeyType::SNAPSHOT_TOMBSTONE)
        .value("LOG_COMPACTED", KeyType::LOG_COMPACTED)
        ;

    py::enum_<OpenMode>(storage, "OpenMode")
        .value("READ", OpenMode::READ)
        .value("WRITE", OpenMode::WRITE)
        .value("DELETE", OpenMode::DELETE);

    storage.def("create_library_index", &create_library_index);

    storage.def("create_mem_config_resolver", [](const py::object & env_config_map_py) -> std::shared_ptr<ConfigResolver> {
        arcticdb::proto::storage::EnvironmentConfigsMap ecm;
        pb_from_python(env_config_map_py, ecm);
        auto resolver = std::make_shared<storage::details::InMemoryConfigResolver>();
        for(auto &[env, cfg] :ecm.env_by_id()){
            EnvironmentName env_name{env};
            for(auto &[id, storage]: cfg.storage_by_id()){
                resolver->add_storage(env_name, StorageName{id}, storage);
            }
            for(auto &[id, lib_desc]: cfg.lib_by_path()){
                resolver->add_library(env_name, lib_desc);
            }
        }
        return resolver;
    });

    py::class_<ConfigResolver, std::shared_ptr<ConfigResolver>>(storage, "ConfigResolver");

    py::class_<Library, std::shared_ptr<Library>>(storage, "Library")
        .def_property_readonly("library_path", [](Library &library){ return library.library_path().to_delim_path(); })
        .def_property_readonly("open_mode", [](Library &library){ return library.open_mode(); })
        .def_property_readonly("config", [](const Library & library)->py::object{
            return util::variant_match(library.config(),
                                       [](const arcticdb::proto::storage::VersionStoreConfig & cfg){
                                           return pb_to_python(cfg);
                                       },
                                       [](const std::monostate & ){
                                            auto none = py::none{};
                                            none.inc_ref();
                                            return none.cast<py::object>();
                                       });
        })
        ;

    py::class_<LibraryManager, std::shared_ptr<LibraryManager>>(storage, "LibraryManager")
        .def(py::init<std::shared_ptr<storage::Library>>())
        .def("write_library_config", [](LibraryManager& library_manager, py::object& lib_cfg, std::string_view library_path) {
            LibraryPath lib_path{library_path, '.'};

            return library_manager.write_library_config(lib_cfg, lib_path);
        })
        .def("get_library_config", [](LibraryManager& library_manager, std::string_view library_path){
            return library_manager.get_library_config(LibraryPath{library_path, '.'});
        })
        .def("remove_library_config", [](LibraryManager& library_manager, std::string_view library_path){
            return library_manager.remove_library_config(LibraryPath{library_path, '.'});
        })
        .def("get_library", [](LibraryManager& library_manager, std::string_view library_path){
            return library_manager.get_library(LibraryPath{library_path, '.'});
        })
        .def("has_library", [](LibraryManager& library_manager, std::string_view library_path){
            return library_manager.has_library(LibraryPath{library_path, '.'});
        })
        .def("list_libraries", [](LibraryManager& library_manager){
            std::vector<std::string> res;
            for(auto & lp:library_manager.get_library_paths()){
                res.push_back(lp.to_delim_path());
            }
            return res;
        });

    py::class_<LibraryIndex, std::shared_ptr<LibraryIndex>>(storage, "LibraryIndex")
        .def(py::init<>([](const std::string &environment_name) {
                 auto resolver = std::make_shared<details::InMemoryConfigResolver>();
                 return std::make_unique<LibraryIndex>(EnvironmentName{environment_name}, resolver);
             })
        )
        .def_static("create_from_resolver", [](const std::string &environment_name, std::shared_ptr<ConfigResolver> resolver){
            return std::make_shared<LibraryIndex>(EnvironmentName{environment_name}, resolver);
        })
        .def("list_libraries", [](LibraryIndex &library_index, std::string_view prefix = ""){
            std::vector<std::string> res;
            for(auto & lp:library_index.list_libraries(prefix)){
                res.push_back(lp.to_delim_path());
            }
            return res;
        } )
        .def("get_library", [](LibraryIndex &library_index, const std::string &library_path, OpenMode open_mode = OpenMode::DELETE) {
            LibraryPath path = LibraryPath::from_delim_path(library_path);
            return library_index.get_library(path, open_mode, UserAuth{});
        })
        ;

    py::register_exception<DuplicateKeyException>(storage, "DuplicateKeyException");
    py::register_exception<NoDataFoundException>(storage, "NoDataFoundException");
    py::register_exception<PermissionException>(storage, "PermissionException");
}

} // namespace arcticdb::storage::apy

