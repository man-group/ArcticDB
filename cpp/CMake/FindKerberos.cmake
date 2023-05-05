# copied from: https://github.com/apache/impala/blob/master/cmake_modules/FindKerberos. 
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Find the native Kerberos includes and library
#
#  KERBEROS_INCLUDE_DIR  - Where to find krb5.h, etc.
#  KERBEROS_LIBRARY      - List of libraries when using krb5.
#  KERBEROS_FOUND        - True if krb5 found.

set(_KRB5_SEARCH_DIRS)
if (KRB5_ROOT)
  set(_KRB5_SEARCH_DIRS PATHS ${KRB5_ROOT} NO_DEFAULT_PATH)
endif()

find_path(KERBEROS_INCLUDE_DIR krb5.h ${_KRB5_SEARCH_DIRS} PATH_SUFFIXES include)
find_library(KERBEROS_LIBRARY NAMES krb5 ${_KRB5_SEARCH_DIRS} PATH_SUFFIXES lib)

# handle the QUIETLY and REQUIRED arguments and set KERBEROS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Kerberos DEFAULT_MSG KERBEROS_LIBRARY KERBEROS_INCLUDE_DIR)