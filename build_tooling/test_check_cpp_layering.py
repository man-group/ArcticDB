"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
from pathlib import Path

import check_cpp_layering as layering

REPO_ROOT = Path(__file__).resolve().parent.parent


def _write(root, rel_path, text):
    path = root / "cpp" / "arcticdb" / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _fake_tree(root):
    _write(root, "util/a.hpp", "#pragma once\n")
    _write(root, "entity/key.hpp", '#pragma once\n#include "arcticdb/util/a.hpp"\n#include "arcticdb/entity/x.hpp"\n')
    _write(root, "codec/codec.cpp", "#include <arcticdb/entity/key.hpp>\n")
    _write(root, "util/test/test_a.cpp", '#include "arcticdb/codec/codec.hpp"\n')
    _write(root, "util/benchmark_a.cpp", '#include "arcticdb/codec/codec.hpp"\n')
    _write(root, "util/notes.txt", '#include "arcticdb/codec/codec.hpp"\n')
    _write(root, "util/commented.cpp", '// #include "arcticdb/codec/codec.hpp"\n')
    allowlist = root / "allowlist.txt"
    allowlist.write_text("# header\ncodec -> entity\nentity -> util\n")
    return allowlist


def _run(root, allowlist, *extra):
    return layering.main(["--root", str(root), "--allowlist", str(allowlist), *extra])


def test_unchanged_edges_pass(tmp_path, capsys):
    allowlist = _fake_tree(tmp_path)
    assert _run(tmp_path, allowlist) == 0
    assert "2 module edges" in capsys.readouterr().out


def test_both_include_spellings_and_only_production_files_are_counted(tmp_path):
    _fake_tree(tmp_path)
    edges = layering.collect_edges(tmp_path)
    assert set(edges) == {("entity", "util"), ("codec", "entity")}
    assert edges[("codec", "entity")] == [(Path("cpp/arcticdb/codec/codec.cpp"), 1)]


def test_new_edge_fails_and_lists_include_sites(tmp_path, capsys):
    allowlist = _fake_tree(tmp_path)
    _write(tmp_path, "util/b.cpp", '#include "arcticdb/util/a.hpp"\n\n#include "arcticdb/version/v.hpp"\n')
    assert _run(tmp_path, allowlist) == 1
    out = capsys.readouterr().out
    assert "util -> version" in out
    assert "cpp/arcticdb/util/b.cpp:3" in out
    assert "no longer exist" not in out


def test_removed_edge_fails(tmp_path, capsys):
    allowlist = _fake_tree(tmp_path)
    _write(tmp_path, "codec/codec.cpp", "#include <arcticdb/util/a.hpp>\n")
    allowlist.write_text("codec -> entity\ncodec -> util\nentity -> util\n")
    assert _run(tmp_path, allowlist) == 1
    out = capsys.readouterr().out
    assert "Remove these lines" in out
    assert "  codec -> entity" in out
    assert "New C++ module dependencies" not in out


def test_update_rewrites_allowlist(tmp_path):
    allowlist = _fake_tree(tmp_path)
    _write(tmp_path, "util/b.cpp", '#include "arcticdb/version/v.hpp"\n')
    assert _run(tmp_path, allowlist, "--update") == 0
    lines = [line for line in allowlist.read_text().splitlines() if line and not line.startswith("#")]
    assert lines == ["codec -> entity", "entity -> util", "util -> version"]
    assert _run(tmp_path, allowlist) == 0


def test_repository_allowlist_is_up_to_date():
    start = time.monotonic()
    assert layering.main(["--root", str(REPO_ROOT)]) == 0
    assert time.monotonic() - start < 2
