# detect if foll version >= 2023.02.13
find_file(FOLLY_THREAD_FACTORY_H "folly/executors/thread_factory/ThreadFactory.h" 
    HINTS
    ${FOLLY_INCLUDE_DIR}
    PATHS
    ${FOLLY_INCLUDE_DIR}
    REQUIRED
)

file(READ ${FOLLY_THREAD_FACTORY_H}  TMPTXT)

SET(STRING_TO_FIND "virtual const std::string& getNamePrefix()")
string(FIND "${TMPTXT}" ${STRING_TO_FIND} matchres)

if(${matchres} EQUAL -1)
    message(STATUS "FOLLY_VERSION_GEQ_2023_02_13 is FALSE")
    SET(FOLLY_VERSION_GEQ_2023_02_13 FALSE)
else()
    add_compile_definitions(FOLLY_VERSION_GEQ_2023_02_13)
    message(STATUS "FOLLY_VERSION_GEQ_2023_02_13 is TRUE")
    SET(FOLLY_GEQ_2023_02_13 TRUE)
endif ()
 