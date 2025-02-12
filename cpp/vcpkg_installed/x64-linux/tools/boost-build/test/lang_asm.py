#!/usr/bin/python

# Copyright 2023 Nikita Kniazev
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.txt or https://www.bfgroup.xyz/b2/LICENSE.txt)

# Test that assembler file is actually C-preprocessed.

import BoostBuild

t = BoostBuild.Tester(use_test_config=False)

t.write("jamroot.jam", """\
obj hello : hello.S : <define>OK ;
""")

'''
'''
t.write("hello.S", """\
#ifndef __ASSEMBLER__
# error __ASSEMBLER__ expected to be defined
#endif
#if __ASSEMBLER__ != 1
# error __ASSEMBLER__ expected to be defined to value of 1
#endif
#ifndef OK
# error OK was not defined
#endif
// MASM/ARMASM requires END token at the end of the file
#ifdef __GNUC__
# define END
#endif
// whitespace before END is to stop MARMASM from barking 'warning A4045: missing END directive'
 END
""")

t.run_build_system(["-d+2", "--debug-configuration"])
t.expect_addition("bin/$toolset/debug*/hello.obj")

t.cleanup()
