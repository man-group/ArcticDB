/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/python_bindings_common.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/ref_key.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/operators.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace arcticdb::entity::apy {

void register_common_entity_bindings(py::module& m, BindingScope scope) {
    bool local_bindings = (scope == BindingScope::LOCAL);

    py::class_<RefKey, std::shared_ptr<RefKey>>(m, "RefKey", py::module_local(local_bindings))
            .def(py::init())
            .def(py::init<StreamId, KeyType>())
            .def_property_readonly("id", &RefKey::id)
            .def_property_readonly("type", [](const RefKey& self) { return self.type(); })
            .def(pybind11::self == pybind11::self)
            .def(pybind11::self != pybind11::self)
            .def("__repr__", &RefKey::view);

    py::class_<AtomKey, std::shared_ptr<AtomKey>>(m, "AtomKey", py::module_local(local_bindings))
            .def(py::init())
            .def(py::init<StreamId, VersionId, timestamp, ContentHash, IndexValue, IndexValue, KeyType>())
            .def("change_id", &AtomKey::change_id)
            .def_property_readonly("id", &AtomKey::id)
            .def_property_readonly("version_id", &AtomKey::version_id)
            .def_property_readonly("creation_ts", &AtomKey::creation_ts)
            .def_property_readonly("content_hash", &AtomKey::content_hash)
            .def_property_readonly("start_index", &AtomKey::start_index)
            .def_property_readonly("end_index", &AtomKey::end_index)
            .def_property_readonly("type", [](const AtomKey& self) { return self.type(); })
            .def(pybind11::self == pybind11::self)
            .def(pybind11::self != pybind11::self)
            .def("__repr__", &AtomKey::view_human)
            .def(py::self < py::self)
            .def(py::pickle(
                    [](const AtomKey& key) {
                        constexpr int serialization_version = 0;
                        return py::make_tuple(
                                serialization_version,
                                key.id(),
                                key.version_id(),
                                key.creation_ts(),
                                key.content_hash(),
                                key.start_index(),
                                key.end_index(),
                                key.type()
                        );
                    },
                    [](py::tuple t) {
                        util::check(t.size() >= 7, "Invalid AtomKey pickle object!");

                        AtomKey key(
                                t[1].cast<StreamId>(),
                                t[2].cast<VersionId>(),
                                t[3].cast<timestamp>(),
                                t[4].cast<ContentHash>(),
                                t[5].cast<IndexValue>(),
                                t[6].cast<IndexValue>(),
                                t[7].cast<KeyType>()
                        );
                        return key;
                    }
            ));

    py::class_<VersionedItem>(m, "VersionedItem", py::module_local(local_bindings))
            .def_property_readonly("symbol", &VersionedItem::symbol)
            .def_property_readonly("timestamp", &VersionedItem::timestamp)
            .def_property_readonly("version", &VersionedItem::version);
}

} // namespace arcticdb::entity::apy
