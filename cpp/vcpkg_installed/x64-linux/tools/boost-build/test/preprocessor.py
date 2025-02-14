#!/usr/bin/env python3

# Copyright 2003 Vladimir Prus
# Copyright 2011 Steven Watanabe
# Copyright 2023 Nikita Kniazev
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or https://www.bfgroup.xyz/b2/LICENSE.txt)

# Test the C/C++ preprocessor.

import BoostBuild

t = BoostBuild.Tester()

t.write("jamroot.jam", """
project ;
preprocessed hello : hello.cpp ;
preprocessed a : a.c ;
exe test : hello a : <define>FAIL ;
preprocessed nolinemarkers-cpp : hello.cpp : <linemarkers>off ;
preprocessed nolinemarkers-c : a.c : <linemarkers>off ;
""")

t.write("hello.cpp", """
#ifndef __cplusplus
#error "This file must be compiled as C++"
#endif
#ifdef FAIL
#error "Not preprocessed?"
#endif
extern "C" int foo();
int main() { return foo(); }
""")

t.write("a.c", """
/* This will not compile unless in C mode. */
#ifdef __cplusplus
#error "This file must be compiled as C"
#endif
#ifdef FAIL
#error "Not preprocessed?"
#endif
int foo(void)
{
    int new = 0;
    new = (new+1)*7;
    return new;
}
""")

t.run_build_system(["-d+2"])
t.expect_addition("bin/$toolset/debug*/hello.ii")
t.expect_addition("bin/$toolset/debug*/a.i")
t.expect_addition("bin/$toolset/debug*/nolinemarkers-cpp.ii")
t.expect_addition("bin/$toolset/debug*/nolinemarkers-c.i")
t.expect_addition("bin/$toolset/debug*/test.exe")
t.fail_test('#' in t.read("bin/$toolset/debug*/nolinemarkers-cpp.ii"))
t.fail_test('#' in t.read("bin/$toolset/debug*/nolinemarkers-c.i"))

t.cleanup()
