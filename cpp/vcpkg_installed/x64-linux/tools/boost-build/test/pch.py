#!/usr/bin/env python3

# Copyright 2006 Vladimir Prus.
# Copyright Nikita Kniazev 2020.
# Distributed under the Boost Software License, Version 1.0. (See
# accompanying file LICENSE.txt or copy at
# https://www.bfgroup.xyz/b2/LICENSE.txt)

import BoostBuild
from time import sleep


t = BoostBuild.Tester()

t.write("jamroot.jam", """
import pch ;
project : requirements <warnings-as-errors>on ;
cpp-pch pch : pch.hpp ;
cpp-pch pch-afx : pch.hpp : <define>HELLO ;
cpp-pch pch-msvc-source : pch.hpp : <toolset>msvc:<source>pch.cpp ;
exe hello : hello.cpp pch ;
exe hello-afx : hello-afx.cpp pch-afx : <define>HELLO ;
exe hello-msvc-source : hello-msvc-source.cpp pch-msvc-source ;

cpp-pch subdir-pch : a/pch_different.hpp ;
exe hello-subdir : a/hello.cpp subdir-pch ;
""")

pch_content = """\
#undef HELLO
class TestClass
{
public:
    TestClass( int, int ) {}
};
"""
t.write("pch.hpp", pch_content)
t.copy("pch.hpp", "a/pch_different.hpp")

t.write("pch.cpp", """#include <pch.hpp>
""")

for name in ("hello.cpp", "hello-afx.cpp", "hello-msvc-source.cpp", "a/hello.cpp", "a/hello-rel.cpp"):
    t.write(name, """int main() { TestClass c(1, 2); }
""")

t.run_build_system()
t.expect_addition("bin/$toolset/debug*/hello.exe")
t.expect_addition("bin/$toolset/debug*/hello-afx.exe")
t.expect_addition("bin/$toolset/debug*/hello-msvc-source.exe")
t.expect_addition("bin/$toolset/debug*/hello-subdir.exe")


# Now make the header unusable, replace its content with some garbage, but
# preserve the size and timestamp to fool the compiler. If everything is OK,
# B2 will not recreate PCH, and compiler will happily use pre-compiled
# header, not noticing that the real header is bad.

t.rename("pch.hpp", "pch.hpp.orig")
s = """#error PCH REBUILD HAPPEND
THIS WILL NOT COMPILE
"""
t.write("pch.hpp", s + (len(pch_content) - len(s)) * 'x')
t.copy_timestamp("pch.hpp.orig", "pch.hpp")

t.rm("bin/$toolset/debug*/hello.obj")
t.rm("bin/$toolset/debug*/hello-afx.obj")
t.rm("bin/$toolset/debug*/hello-msvc-source.obj")

t.run_build_system()
t.expect_addition("bin/$toolset/debug*/hello.obj")
t.expect_addition("bin/$toolset/debug*/hello-afx.obj")
t.expect_addition("bin/$toolset/debug*/hello-msvc-source.obj")

t.rm("bin")
t.copy("a/pch_different.hpp", "pch.hpp")
t.rename("a", "b")
t.write("b/jamfile.jam", """\
import pch ;
project : requirements <warnings-as-errors>on ;
cpp-pch pch : pch_different.hpp ;
exe hello : hello.cpp pch ;
cpp-pch pch-rel : ../pch.hpp ;
exe hello-rel : hello-rel.cpp pch-rel ;
""")
t.run_build_system(["-d+2", "b"])
t.expect_addition("b/bin/$toolset/debug*/hello.exe")
t.expect_addition("b/bin/$toolset/debug*/hello-rel.exe")
t.rm("b/bin")
t.run_build_system(["-d+2"], subdir="b")
t.expect_addition("b/bin/$toolset/debug*/hello.exe")
t.expect_addition("b/bin/$toolset/debug*/hello-rel.exe")

t.cleanup()
