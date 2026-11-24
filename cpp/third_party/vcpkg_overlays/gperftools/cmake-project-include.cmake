if(ANDROID)
    include(CheckFunctionExists)
    check_function_exists("glob" HAVE_GLOB_H)
endif()

# ArcticDB:
# - only export the tc_* entry points, so linking tcmalloc does not replace malloc/free or operator new/delete;
# - no initial-exec TLS, which would mark arcticdb_ext STATIC_TLS, so that its TLS block no longer fits the static TLS
#   space glibc reserves for dlopen'ed modules.
add_compile_definitions(TCMALLOC_SKIP_OVERRIDE TCMALLOC_NO_INITIAL_EXEC_TLS)
