include(sdksCommon)

set(SDK_DEPENDENCY_BUILD_LIST "")

set(NON_GENERATED_CLIENT_LIST access-management text-to-speech core queues s3-encryption identity-management transfer)  ## Manually generated code with a name mimicking client name

if(REGENERATE_CLIENTS OR REGENERATE_DEFAULTS)
    message(STATUS "Checking for SDK generation requirements")
    include(FindJava)

    if(NOT Java_JAVA_EXECUTABLE OR NOT Java_JAVAC_EXECUTABLE)
        message(FATAL_ERROR "Generating SDK clients requires a jdk 1.8 installation")
    endif()

    find_program(MAVEN_PROGRAM mvn)
    if(NOT MAVEN_PROGRAM)
        message(FATAL_ERROR "Generating SDK clients requires a maven installation")
    endif()
endif()


if(BUILD_ONLY)
    # Sanitize input client list, for example removing empty elements
    # that may be introduced by trailing semicolons
    set(BUILD_ONLY ${BUILD_ONLY})
    set(SDK_BUILD_LIST ${BUILD_ONLY})
    foreach(TARGET IN LISTS BUILD_ONLY)
        message(STATUS "Considering ${TARGET}")
        get_dependencies_for_sdk(${TARGET} DEPENDENCY_LIST)
        if(DEPENDENCY_LIST)
            STRING(REPLACE "," ";" LIST_RESULT ${DEPENDENCY_LIST})
            foreach(DEPENDENCY IN LISTS LIST_RESULT)
                list(APPEND SDK_DEPENDENCY_BUILD_LIST ${DEPENDENCY})
            endforeach()
        endif()

        if(ENABLE_TESTING)
            get_dependencies_for_test(${TARGET} DEPENDENCY_LIST)
            if(DEPENDENCY_LIST)
                STRING(REPLACE "," ";" LIST_RESULT ${DEPENDENCY_LIST})
                foreach(DEPENDENCY IN LISTS LIST_RESULT)
                    list(APPEND SDK_DEPENDENCY_BUILD_LIST ${DEPENDENCY})
                endforeach()
            endif()
        endif()
    endforeach()
    LIST(REMOVE_DUPLICATES SDK_BUILD_LIST)
    LIST(REMOVE_DUPLICATES SDK_DEPENDENCY_BUILD_LIST)
else()
    set(TEMP_SDK_BUILD_LIST ${GENERATED_SERVICE_LIST})
    list(APPEND TEMP_SDK_BUILD_LIST "core")

    list(APPEND TEMP_SDK_BUILD_LIST ${HIGH_LEVEL_SDK_LIST})

    set(SDK_BUILD_LIST ${TEMP_SDK_BUILD_LIST})

    # remove any missing targets from the build list, factoring in dependencies appropriately
    foreach(SDK IN LISTS TEMP_SDK_BUILD_LIST)
        set(REMOVE_SDK 0)
        list (FIND NON_GENERATED_CLIENT_LIST ${SDK} _index)
        if (${_index} GREATER -1) # old cmake search in a list syntax
            set(SDK_DIR "src/aws-cpp-sdk-${SDK}")
        else()
            set(SDK_DIR "generated/src/aws-cpp-sdk-${SDK}")
        endif()

        if(NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${SDK_DIR}" AND NOT REGENERATE_CLIENTS)
            set(REMOVE_SDK 1)
        endif()

        if(REMOVE_SDK)
            get_sdks_depending_on(${SDK} REMOVE_LIST)
            foreach(REMOVE_SDK IN LISTS REMOVE_LIST)
                list(REMOVE_ITEM SDK_BUILD_LIST ${REMOVE_SDK})
            endforeach()
        endif()
    endforeach()
endif()


if(ENABLE_VIRTUAL_OPERATIONS) # it could be set to 0/1 or ON/OFF
    set(ENABLE_VIRTUAL_OPERATIONS_ARG "--enable-virtual-operations")
else()
    set(ENABLE_VIRTUAL_OPERATIONS_ARG "")
endif()

if(REGENERATE_CLIENTS OR REGENERATE_DEFAULTS)
    message(STATUS "Regenerating clients/defaults that have been selected for build.")
    if(REGENERATE_CLIENTS AND BUILD_ONLY)
        foreach(build_only ${BUILD_ONLY})
            list (FIND NON_GENERATED_CLIENT_LIST ${build_only} _index)
            if (${_index} GREATER -1) # old cmake search in a list syntax
                message(FATAL_ERROR "Explicitly requested to regenerate non-regeneratable component: ${build_only}")
            endif()
        endforeach()
    endif()

    set(MERGED_BUILD_LIST ${SDK_BUILD_LIST})
    LIST(REMOVE_DUPLICATES MERGED_BUILD_LIST)
    list(REMOVE_ITEM MERGED_BUILD_LIST ${NON_GENERATED_CLIENT_LIST})

    if(REGENERATE_CLIENTS)
        set(MERGED_BUILD_LIST_STR "${MERGED_BUILD_LIST}")
        STRING(REPLACE ";" "," MERGED_BUILD_LIST_STR "${MERGED_BUILD_LIST_STR}")
        set(REGENERATE_CLIENTS_ARG "--client_list")
    else()
        set(REGENERATE_CLIENTS_ARG "")
    endif()

    if(REGENERATE_DEFAULTS)
        set(REGENERATE_DEFAULTS_ARG "--defaults")
    else()
        set(REGENERATE_DEFAULTS_ARG "")
    endif()

    execute_process(
        COMMAND ${PYTHON3_CMD} tools/scripts/run_code_generation.py ${REGENERATE_CLIENTS_ARG} ${MERGED_BUILD_LIST_STR} ${REGENERATE_DEFAULTS_ARG} ${ENABLE_VIRTUAL_OPERATIONS_ARG} --output_location ./generated/
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE ret
    )

    if(ret AND NOT ret EQUAL 0)
        message(FATAL_ERROR "Failed to regenerate code")
    endif()
endif()


#at this point, if user has specified some customized clients, generate them and add them to the build here.
foreach(custom_client ${ADD_CUSTOM_CLIENTS})
    message(STATUS "${custom_client}")
    STRING(REGEX MATCHALL "serviceName=(.*), ?version=(.*)" CLIENT_MATCHES "${custom_client}")
    set(C_LEN 0)
    LIST(LENGTH CLIENT_MATCHES C_LEN)
    if(CMAKE_MATCH_COUNT GREATER 0)
        set(C_SERVICE_NAME ${CMAKE_MATCH_1})
        set(C_VERSION ${CMAKE_MATCH_2})

        set(SDK_C2J_FILE "${CMAKE_CURRENT_SOURCE_DIR}/code-generation/api-descriptions/${C_SERVICE_NAME}-${C_VERSION}.normal.json")
        if(NOT (EXISTS ${SDK_C2J_FILE}))
            message(FATAL_ERROR "${C_SERVICE_NAME} is required for build, but C2J file '${SDK_C2J_FILE}' is missing!")
        endif()

        message(STATUS "Clearing existing directory for ${C_SERVICE_NAME} to prepare for generation.")
        file(REMOVE_RECURSE "${CMAKE_CURRENT_SOURCE_DIR}/aws-cpp-sdk-${C_SERVICE_NAME}")
        message(STATUS "generating client for ${C_SERVICE_NAME} version ${C_VERSION}")
        execute_process(
            COMMAND ${PYTHON3_CMD} tools/scripts/legacy/generate_sdks.py --serviceName ${C_SERVICE_NAME} --apiVersion ${C_VERSION} ${ENABLE_VIRTUAL_OPERATIONS_ARG} --outputLocation ./generated/
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
        LIST(APPEND SDK_BUILD_LIST ${C_SERVICE_NAME})
    endif()
endforeach(custom_client)

# when building a fixed target set, missing SDKs are an error
# when building "everything", we forgive missing SDKs; should this become an option instead?
if(BUILD_ONLY)
    # make sure all the sdks/c2js are present; a missing sdk-directory or c2j file is a build error when building a manually-specified set
    foreach(SDK IN LISTS SDK_BUILD_LIST)
        list (FIND NON_GENERATED_CLIENT_LIST ${SDK} _index)
        if (${_index} GREATER -1) # old cmake search in a list syntax
            set(SDK_DIR "src/aws-cpp-sdk-${SDK}")
        else()
            set(SDK_DIR "generated/src/aws-cpp-sdk-${SDK}")
        endif()

        if(NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${SDK_DIR}")
            message(FATAL_ERROR "${SDK} is required for build, but ${SDK_DIR} directory is missing!")
        endif()
    endforeach()

    set(TEMP_SDK_DEPENDENCY_BUILD_LIST ${SDK_DEPENDENCY_BUILD_LIST})
    foreach (SDK IN LISTS TEMP_SDK_DEPENDENCY_BUILD_LIST)
        list(FIND SDK_BUILD_LIST ${SDK} DEPENDENCY_INDEX)
        if(DEPENDENCY_INDEX LESS 0)
            # test dependencies should also be built from source instead of locating by calling find_package
            # which may cause version conflicts as well as double targeting built targets
            list (FIND NON_GENERATED_CLIENT_LIST ${SDK} _index)
            if (${_index} GREATER -1) # old cmake search in a list syntax
                set(SDK_DIR "src/aws-cpp-sdk-${SDK}")
            else()
                set(SDK_DIR "generated/src/aws-cpp-sdk-${SDK}")
            endif()
            if (NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${SDK_DIR}")
                message(FATAL_ERROR "${SDK} is required for build, but ${SDK_DIR} directory is missing!")
            endif ()
        else ()
            list(REMOVE_ITEM SDK_DEPENDENCY_BUILD_LIST ${SDK})
        endif ()
    endforeach ()

    foreach (SDK IN LISTS SDK_DEPENDENCY_BUILD_LIST)
        list(APPEND SDK_BUILD_LIST "${SDK}")
    endforeach()
endif()

if (BUILD_BENCHMARKS)
    LIST(APPEND SDK_BUILD_LIST "s3;s3-crt;monitoring;core")
    add_subdirectory(tests/benchmark)
endif ()

LIST(REMOVE_DUPLICATES SDK_BUILD_LIST)
LIST(REMOVE_DUPLICATES SDK_DEPENDENCY_BUILD_LIST)

function(add_sdks)
    LIST(APPEND EXPORTS "")
    foreach(SDK IN LISTS SDK_BUILD_LIST)
        message(STATUS "Adding ${SDK} to SDK build")
        set(SDK_TARGET "aws-cpp-sdk-${SDK}")
        list (FIND NON_GENERATED_CLIENT_LIST ${SDK} _index)
        if (${_index} GREATER -1) # old cmake search in a list syntax
            set(SDK_DIR "src/${SDK_TARGET}")
        else()
            set(SDK_DIR "generated/src/${SDK_TARGET}")
        endif()

        add_subdirectory("${SDK_DIR}")
        LIST(APPEND EXPORTS "${SDK_TARGET}")
        unset(SDK_TARGET)
    endforeach()

    #testing
    if(ENABLE_TESTING)
        add_subdirectory(tests/testing-resources)

        # android-unified-tests includes all the tests in our code base, those tests related services may not be incldued in BUILD_ONLY,
        # means, those services will not be built, but will be tried to linked against with test targets, which will cause link time error.
        # See https://github.com/aws/aws-sdk-cpp/issues/786. We should only build android-unified-tests when doing a full build.
        if(PLATFORM_ANDROID AND NOT BUILD_SHARED_LIBS AND NOT BUILD_ONLY)
            add_subdirectory(android-unified-tests)
        else()
            foreach(SDK IN LISTS SDK_BUILD_LIST)
                get_test_projects_for_service(${SDK} TEST_PROJECTS)
                if(TEST_PROJECTS)
                    if (NO_HTTP_CLIENT AND NOT "${SDK}" STREQUAL "core")
                        set(NO_HTTP_CLIENT_SKIP_INTEGRATION_TEST ON)
                        continue()
                    endif()
                    if (NOT ENABLE_VIRTUAL_OPERATIONS)
                        if ("${SDK}" STREQUAL "transfer" OR "${SDK}" STREQUAL "s3-encryption")
                            message(STATUS "Skip building ${SDK} integration tests because some tests need to override service operations, but ENABLE_VIRTUAL_OPERATIONS is switched off.")
                            continue()
                        endif()
                    endif()
                    STRING(REPLACE "," ";" LIST_RESULT "${TEST_PROJECTS}")
                    foreach(TEST_PROJECT IN LISTS LIST_RESULT)
                        if(TEST_PROJECT)
                            # before we add the test, make sure all of its dependencies are present
                            set(ADD_TEST 1)
                            get_dependencies_for_test(${SDK} DEPENDENCY_LIST)
                            if(DEPENDENCY_LIST)
                                STRING(REPLACE "," ";" LIST_RESULT ${DEPENDENCY_LIST})
                                foreach(DEPENDENCY IN LISTS LIST_RESULT)
                                    list(FIND SDK_BUILD_LIST ${DEPENDENCY} DEPENDENCY_INDEX)
                                    if(DEPENDENCY_INDEX LESS 0)
                                        message(STATUS "Removing test project ${TEST_PROJECT} due to missing dependency ${DEPENDENCY}")
                                        set(ADD_TEST 0)
                                    endif()
                                endforeach()
                            endif()

                            if(ADD_TEST)
                                add_subdirectory(${TEST_PROJECT})
                            endif()
                        endif()
                    endforeach()
                endif()
             endforeach()
             if (NO_HTTP_CLIENT_SKIP_INTEGRATION_TEST)
                 message(STATUS "No http client is specified, SDK will not build integration tests")
             endif()
             unset(NO_HTTP_CLIENT_SKIP_INTEGRATION_TEST)
        endif()
    endif()

    # the catch-all config needs to list all the targets in a dependency-sorted order
    include(dependencies)
    sort_links(EXPORTS)

    # make an everything config by just including all the individual configs
    file(WRITE ${CMAKE_BINARY_DIR}/aws-sdk-cpp-config.cmake "")
    foreach(EXPORT_TARGET IN LISTS EXPORTS)
        file(APPEND ${CMAKE_BINARY_DIR}/aws-sdk-cpp-config.cmake "include(\"\${CMAKE_CURRENT_LIST_DIR}/${EXPORT_TARGET}/${EXPORT_TARGET}-config.cmake\")\n")
    endforeach()
endfunction()
