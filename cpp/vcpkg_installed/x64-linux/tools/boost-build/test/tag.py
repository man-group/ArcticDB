#!/usr/bin/env python3

# Copyright (C) 2003. Pedro Ferreira
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or copy at
# https://www.bfgroup.xyz/b2/LICENSE.txt)

import BoostBuild

import re
from functools import reduce

###############################################################################
#
# test_folder_with_dot_in_name()
# ------------------------------
#
###############################################################################

def test_folder_with_dot_in_name(t):
    """
      Regression test: the 'tag' feature did not work in directories that had a
    dot in their name.

    """
    t.write("version-1.32.0/jamroot.jam", """\
project test : requirements <tag>@$(__name__).tag ;

rule tag ( name : type ? : property-set )
{
   # Do nothing, just make sure the rule is invoked OK.
   ECHO The tag rule has been invoked. ;
}
exe a : a.cpp ;
""")
    t.write("version-1.32.0/a.cpp", "int main() {}\n")

    t.run_build_system(subdir="version-1.32.0")
    t.expect_addition("version-1.32.0/bin/$toolset/debug*/a.exe")
    t.expect_output_lines("The tag rule has been invoked.")


###############################################################################
#
# test_tag_property()
# -------------------
#
###############################################################################

def test_tag_property(t):
    """Basic tag property test."""

    t.write("jamroot.jam", """\
import virtual-target ;

rule tag ( name : type ? : property-set )
{
    local tags ;
    switch [ $(property-set).get <variant> ]
    {
        case debug   : tags += d ;
        case release : tags += r ;
    }
    switch [ $(property-set).get <link> ]
    {
        case shared : tags += s ;
        case static : tags += t ;
    }
    switch $(type)
    {
        case SHARED_LIB : tags += _dll ;
        case STATIC_LIB : tags += _lib ;
        case EXE        : tags += _exe ;
    }
    if $(tags)
    {
        return [ virtual-target.add-prefix-and-suffix $(name)_$(tags:J="")
            : $(type) : $(property-set) ] ;
    }
}

# Test both fully-qualified and local name of the rule
exe a : a.cpp : <tag>@$(__name__).tag ;
lib b : a.cpp : <tag>@tag ;
stage c : a ;
""")

    t.write("a.cpp", """\
int main() {}
""")

    file_list = {
        "bin/$toolset/debug*/a_ds_exe.exe",
        "bin/$toolset/debug*/b_ds_dll.dll",
        "c/a_ds_exe.exe",
        "bin/$toolset/release*/a_rs_exe.exe",
        "bin/$toolset/release*/b_rs_dll.dll",
        "c/a_rs_exe.exe",
        "bin/$toolset/debug*/a_dt_exe.exe",
        "bin/$toolset/debug*/b_dt_lib.lib",
        "c/a_dt_exe.exe",
        "bin/$toolset/release*/a_rt_exe.exe",
        "bin/$toolset/release*/b_rt_lib.lib",
        "c/a_rt_exe.exe",
    }
    if t.is_pdb_expected():
        file_list |= {re.sub(r'(_\w*d\w*)\.(exe|dll)$', r'\1.pdb', file)
                      for file in file_list}
    file_list = list(reduce(lambda x, y: x+y, map(BoostBuild.List, file_list)))

    variants = ["debug", "release", "link=static,shared", "debug-symbols=on"]

    t.run_build_system(variants)
    t.expect_addition(file_list)

    t.run_build_system(variants + ["clean"])
    t.expect_removal(file_list)


###############################################################################
#
# main()
# ------
#
###############################################################################

t = BoostBuild.Tester(use_test_config=False)

test_tag_property(t)
test_folder_with_dot_in_name(t)

t.cleanup()
