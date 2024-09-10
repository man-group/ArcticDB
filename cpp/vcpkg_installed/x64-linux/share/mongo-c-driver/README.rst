==============
mongo-c-driver
==============

About
=====

mongo-c-driver is a project that includes two libraries:

- libmongoc, a client library written in C for MongoDB.
- libbson, a library providing useful routines related to building, parsing, and iterating BSON documents.

If libmongoc is not needed, it is possible to build and install only libbson.

Documentation / Support / Feedback
==================================

The documentation is available at `MongoDB C Driver Docs <https://www.mongodb.com/docs/drivers/c/>`_ and https://www.mongoc.org.
For issues with, questions about, or feedback for libmongoc, please look into
our `support channels <http://www.mongodb.org/about/support>`_. Please
do not email any of the libmongoc developers directly with issues or
questions - you're more likely to get an answer on the `MongoDB Community Forums`_ or `StackOverflow <https://stackoverflow.com/questions/tagged/mongodb+c>`_.

Bugs / Feature Requests
=======================

Think you’ve found a bug? Want to see a new feature in libmongoc? Please open a
case in our issue management tool, JIRA:

- `Create an account and login <https://jira.mongodb.org>`_.
- Navigate to `the CDRIVER project <https://jira.mongodb.org/browse/CDRIVER>`_.
- Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.

Bug reports in JIRA for all driver projects (i.e. CDRIVER, CSHARP, JAVA) and the
Core Server (i.e. SERVER) project are **public**.

How To Ask For Help
-------------------

If you are having difficulty building the driver after reading the below instructions, please post on
the `MongoDB Community Forums`_ to ask for help. Please include in your post all of the following
information:

- The version of the driver you are trying to build (branch or tag).
    - Examples: ``r1.17`` (branch), ``1.9.5`` (tag)
- Host OS, version, and architecture.
    - Examples: Windows 10 64-bit x86, Ubuntu 16.04 64-bit x86, macOS 11.0
- C Compiler and version.
    - Examples: GCC 7.3.0, Visual Studio Community 2017, clang 3.9, XCode 9.3
- Run CMake with ``--log-level=debug`` and ``--log-context`` for more verbose output.
- The output of any ``cmake``, ``make``, or other commands executed during the build.
- The text of the error you encountered.

Failure to include the relevant information will delay a useful response.
Here is a made-up example of a help request that provides the relevant
information:

  Hello, I'm trying to build the C driver with Kerberos support, from
  mongo-c-driver-1.9.5.tar.gz. I'm on Ubuntu 16.04, 64-bit Intel, with gcc
  5.4.0. I run CMake like::

    $ cmake .
    -- The C compiler identification is ;GNU 5.4.0
    -- Check for working C compiler: /usr/bin/cc
    -- Check for working C compiler: /usr/bin/cc -- works

    ... SNIPPED OUTPUT, but when you ask for help, include full output without any omissions ...

    -- Searching for libsasl2
    --   Not found (specify -DCMAKE_LIBRARY_PATH=/path/to/sasl/lib for SASL support)
    CMake Error at CMakeLists.txt:10 (_message):
        SASL not found

  Can you tell me what I need to install? Thanks!

.. _MongoDB Community Forums: https://www.mongodb.com/community/forums/tags/c/data/drivers/7/c-driver

Security Vulnerabilities
------------------------

If you’ve identified a security vulnerability in a driver or any other
MongoDB project, please report it according to the `instructions here
<https://www.mongodb.com/docs/manual/tutorial/create-a-vulnerability-report>`_.


Installation
============

Detailed installation instructions are in the manual:
https://www.mongodb.com/docs/languages/c/c-driver/current/libmongoc/tutorials/obtaining-libraries/


Resources
============

* `Getting Started Tutorial <https://www.mongodb.com/docs/languages/c/c-driver/current/libmongoc/tutorial>`_.
* `MongoDB C Driver Examples <https://github.com/mongodb/mongo-c-driver/tree/master/src/libmongoc/examples>`_.
* Tutorials, videos, and code examples using the MongoDB C Driver can also be found in the `MongoDB Developer Center <https://www.mongodb.com/developer/languages/c/>`_.
