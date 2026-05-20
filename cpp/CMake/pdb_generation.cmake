set(_ARCTICDB_PDB_GENERATION_MODE_WARNED OFF CACHE INTERNAL "")
# In case ARCTICDB_PDB_GENERATION_MODE is TU and MSVC is used this will set a custom PDB path for each translation
# unit. All translation units will produce unique PDB. All PDBs are automatically linked by the linker at the end (no
# action required). The name of the PDB is the name of the cpp file_<MD5 hash of the path to the file>.pdb because by
# default all PDBs are stored in the same folder and we need to distinguish files with the same name but from different
# folders e.g. (a/b.cpp and c/b.cpp) will both be placed in the same folder with name b.cpp and this will confuse the
# linker and the debugger. Attaching the path as MD5 hash will make files named the same way unique as long as they have
# different path. If ARCTICDB_PDB_GENERATION_MODE is TARGET (which is both the default for ArcticDB and MSVC) there will
# be one PDB per target and in case of parallel builds each compiler invocation will try taking a lock and appending in
# the PDB file this makes the output of .cl nondeterministic and compiler caches as sccache or ccache will not work.

# While in theory ARCTICDB_PDB_GENERATION_MODE=TU should make parallel builds faster it forces MSBuild to run
# single-threaded builds even if \MP is set. This is because it batches building of TUs based on compiler parameters
# as each TU now has unique PDB all TUs will have different flags resulting of no batches. Note: Ninja does not have
# this problem.

# Pre-compiled headers cannot work with ARCTICDB_PDB_GENERATION_MODE=TU

# ARCTICDB_PDB_GENERATION_MODE=TU makes sense only when CMAKE_MSVC_DEBUG_INFORMATION_FORMAT=ProgramDatabase which is
# the same as setting the /Zi (or /ZI) compiler flag.

# The function takes the path to each translation unit. The path can be relative or absolute. All paths must be unique.
function(set_pdb_name_per_translation_unit)
    if(MSVC AND "${ARCTICDB_PDB_GENERATION_MODE}" STREQUAL "TU")
        if(NOT "${CMAKE_MSVC_DEBUG_INFORMATION_FORMAT}" STREQUAL "ProgramDatabase")
            message(WARNING
                "ARCTICDB_PDB_GENERATION_MODE set to ${ARCTICDB_PDB_GENERATION_MODE}, but "
                "CMAKE_MSVC_DEBUG_INFORMATION_FORMAT is ${CMAKE_MSVC_DEBUG_INFORMATION_FORMAT} (not ProgramDatabase). "
                "Generation of PDBs per TU makes sense only if debug format is ProgramDatabase (/Zi or /ZI are set)"
            )
            return()
        endif()
        message("ArcticDB MSVC build will generate one PDB per translation unit")

        if(ARCTICDB_USE_PCH)
            message(FATAL_ERROR "Cannot generate .pdb per translation unit and use precompiled headers")
        endif()

        if(CMAKE_GENERATOR MATCHES "Visual Studio" AND NOT _ARCTICDB_PDB_GENERATION_MODE_WARNED)
            message(AUTHOR_WARNING
                "Setting ARCTICDB_PDB_GENERATION_MODE to TU will result in single threaded builds with MSBuild even "
                "if /MP is set"
            )
            set(_ARCTICDB_PDB_GENERATION_MODE_WARNED ON PARENT_SCOPE)
        endif()

        if(CMAKE_CONFIGURATION_TYPES)
            foreach(config IN LISTS CMAKE_CONFIGURATION_TYPES)
                if(NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${config})
                    message("ArcticDB creating folder ${CMAKE_CURRENT_BINARY_DIR}/${config} to store .pdb files")
                    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${config}")
                endif()
            endforeach()
        else()
            if(CMAKE_BUILD_TYPE)
                if(NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_BUILD_TYPE})
                    message("ArcticDB creating folder ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_BUILD_TYPE} to store .pdb files")
                    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_BUILD_TYPE}")
                endif()
            else()
                message(FATAL_ERROR "CMAKE_BUILD_TYPE not set") 
            endif()
        endif()

        foreach(src IN LISTS ARGN)
            get_filename_component(filename ${src} NAME_WE)
            string(MD5 src_hash ${src})
            set(new_pdb_name "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/${filename}_${src_hash}.pdb")
            set_property(SOURCE ${src} APPEND PROPERTY COMPILE_OPTIONS "/Fd${new_pdb_name}")
        endforeach()
    endif()
endfunction()

# CMAKE_C_COMPILE_OBJECT and CMAKE_CXX_COMPILE_OBJECT are a general template for how to invoke the compiler to compile
# a source file. It overwrites the /Fd flag even when set_pdb_name_per_translation_unit is used, preventing generation
# of PDB per TU and preventing the use of compiler caches. This function will strip the flag and will set the parent
# scope of CMAKE_C_COMPILE_OBJECT and CMAKE_CXX_COMPILE_OBJECT. Must be called once per CMakeLists.txt using
# set_pdb_name_per_translation_unit.
function(strip_fd_from_compile_object)
    if(MSVC AND "${ARCTICDB_PDB_GENERATION_MODE}" STREQUAL "TU")
        string(REPLACE "/Fd<TARGET_COMPILE_PDB>" "" CMAKE_CXX_COMPILE_OBJECT "${CMAKE_CXX_COMPILE_OBJECT}")
        string(REPLACE "/Fd<TARGET_COMPILE_PDB>" "" CMAKE_C_COMPILE_OBJECT "${CMAKE_C_COMPILE_OBJECT}")
        set(CMAKE_C_COMPILE_OBJECT "${CMAKE_C_COMPILE_OBJECT}" PARENT_SCOPE)
        set(CMAKE_CXX_COMPILE_OBJECT "${CMAKE_CXX_COMPILE_OBJECT}" PARENT_SCOPE)
    endif()
endfunction()