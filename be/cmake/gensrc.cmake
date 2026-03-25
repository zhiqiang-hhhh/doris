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

# CMake module for generating source code from proto and thrift files.
# This replaces the legacy make-based approach with CMake's native
# add_custom_command for proper incremental build support.

set(GENSRC_OUTPUT_DIR "${CMAKE_BINARY_DIR}/gensrc/gen_cpp")
file(MAKE_DIRECTORY ${GENSRC_OUTPUT_DIR})

set(PROTOC_EXECUTABLE "${THIRDPARTY_DIR}/bin/protoc")
# Use THRIFT_COMPILER from parent CMakeLists.txt if available, otherwise fall back to THIRDPARTY_DIR
if(THRIFT_COMPILER)
    set(THRIFT_EXECUTABLE "${THRIFT_COMPILER}")
else()
    set(THRIFT_EXECUTABLE "${THIRDPARTY_DIR}/bin/thrift")
endif()

# Set paths to source directories
set(PROTO_SOURCE_DIR "${CMAKE_SOURCE_DIR}/../gensrc/proto")
set(THRIFT_SOURCE_DIR "${CMAKE_SOURCE_DIR}/../gensrc/thrift")
set(SCRIPT_SOURCE_DIR "${CMAKE_SOURCE_DIR}/../gensrc/script")

# Find all proto files
file(GLOB_RECURSE PROTO_FILES "${PROTO_SOURCE_DIR}/*.proto")
list(LENGTH PROTO_FILES PROTO_FILE_COUNT)
message(STATUS "GENSRC: Found ${PROTO_FILE_COUNT} proto files")
message(STATUS "GENSRC: Proto source dir: ${PROTO_SOURCE_DIR}")

# Find all thrift files  
file(GLOB_RECURSE THRIFT_FILES "${THRIFT_SOURCE_DIR}/*.thrift")
list(LENGTH THRIFT_FILES THRIFT_FILE_COUNT)
message(STATUS "GENSRC: Found ${THRIFT_FILE_COUNT} thrift files")
message(STATUS "GENSRC: Thrift source dir: ${THRIFT_SOURCE_DIR}")

# Store all generated source files for dependency tracking
set(GENSRC_PROTO_CC_FILES "")
set(GENSRC_THRIFT_CC_FILES "")
set(GENSRC_GENERATED_FILES "")

# Generate proto files
foreach(PROTO ${PROTO_FILES})
    get_filename_component(BASE_NAME ${PROTO} NAME_WE)
    set(GEN_CC "${GENSRC_OUTPUT_DIR}/${BASE_NAME}.pb.cc")
    set(GEN_H "${GENSRC_OUTPUT_DIR}/${BASE_NAME}.pb.h")
    
    add_custom_command(
        OUTPUT ${GEN_CC} ${GEN_H}
        COMMAND ${PROTOC_EXECUTABLE}
            ARGS --proto_path=${PROTO_SOURCE_DIR}
                 --cpp_out=${GENSRC_OUTPUT_DIR}
                 ${PROTO}
        DEPENDS ${PROTO} ${PROTOC_EXECUTABLE}
        COMMENT "Generating C++ from proto: ${BASE_NAME}"
        VERBATIM
    )
    
    list(APPEND GENSRC_PROTO_CC_FILES ${GEN_CC})
    list(APPEND GENSRC_GENERATED_FILES ${GEN_CC} ${GEN_H})
endforeach()

# Generate thrift files (C++ only)
foreach(THRIFT ${THRIFT_FILES})
    get_filename_component(BASE_NAME ${THRIFT} NAME_WE)
    
    set(THRIFT_ARGS 
        -I ${THRIFT_SOURCE_DIR}
        -I "${CMAKE_SOURCE_DIR}/../gensrc/build/thrift"
        --gen cpp:moveable_types,no_skeleton
        -out ${GENSRC_OUTPUT_DIR}
        --allow-64bit-consts
        -strict
    )
    
    set(GEN_TYPES_CC "${GENSRC_OUTPUT_DIR}/${BASE_NAME}_types.cpp")
    set(GEN_TYPES_H "${GENSRC_OUTPUT_DIR}/${BASE_NAME}_types.h")
    
    add_custom_command(
        OUTPUT ${GEN_TYPES_CC} ${GEN_TYPES_H}
        COMMAND ${THRIFT_EXECUTABLE}
            ARGS ${THRIFT_ARGS} ${THRIFT}
        DEPENDS ${THRIFT} ${THRIFT_EXECUTABLE}
        COMMENT "Generating C++ from thrift: ${BASE_NAME}"
        VERBATIM
    )
    
    list(APPEND GENSRC_THRIFT_CC_FILES ${GEN_TYPES_CC})
    list(APPEND GENSRC_GENERATED_FILES ${GEN_TYPES_CC} ${GEN_TYPES_H})
endforeach()

# Add version.h generation
set(VERSION_H "${GENSRC_OUTPUT_DIR}/version.h")
add_custom_command(
    OUTPUT ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "/* This file is auto-generated */" > ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#ifndef DORIS_VERSION_H" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define DORIS_VERSION_H" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define ROWayne_VERSION \"${DORIS_BUILD_VERSION}\"" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define ROWayne_VERSION_MAJOR ${DORIS_BUILD_VERSION_MAJOR}" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define ROWayne_VERSION_MINOR ${DORIS_BUILD_VERSION_MINOR}" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define ROWayne_VERSION_PATCH ${DORIS_BUILD_VERSION_PATCH}" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define COMMIT_ID \"${DORIS_BUILD_COMMIT_HASH}\"" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define BUILD_TIME \"${DORIS_BUILD_TIME}\"" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#define BUILD_TYPE \"${CMAKE_BUILD_TYPE}\"" >> ${VERSION_H}
    COMMAND 
        ${CMAKE_COMMAND} -E echo "#endif" >> ${VERSION_H}
    DEPENDS
    COMMENT "Generating version.h"
)
list(APPEND GENSRC_GENERATED_FILES ${VERSION_H})

# Add cloud_version.h generation (if cloud is enabled)
if(ENABLE_CLOUD)
    set(CLOUD_VERSION_H "${GENSRC_OUTPUT_DIR}/cloud_version.h")
    add_custom_command(
        OUTPUT ${CLOUD_VERSION_H}
        COMMAND 
            ${CMAKE_COMMAND} -E echo "/* This file is auto-generated for cloud */" > ${CLOUD_VERSION_H}
        COMMAND 
            ${CMAKE_COMMAND} -E echo "#ifndef DORIS_CLOUD_VERSION_H" >> ${CLOUD_VERSION_H}
        COMMAND 
            ${CMAKE_COMMAND} -E echo "#define DORIS_CLOUD_VERSION_H" >> ${CLOUD_VERSION_H}
        COMMAND 
            ${CMAKE_COMMAND} -E echo "#define CLOUD_VERSION \"${DORIS_CLOUD_VERSION}\"" >> ${CLOUD_VERSION_H}
        COMMAND 
            ${CMAKE_COMMAND} -E echo "#endif" >> ${CLOUD_VERSION_H}
        DEPENDS
        COMMENT "Generating cloud_version.h"
    )
    list(APPEND GENSRC_GENERATED_FILES ${CLOUD_VERSION_H})
endif()

# Add opcode generation (functions.cc, functions.h)
file(MAKE_DIRECTORY "${GENSRC_OUTPUT_DIR}/opcode")
set(FUNCTIONS_CC "${GENSRC_OUTPUT_DIR}/opcode/functions.cc")
set(FUNCTIONS_H "${GENSRC_OUTPUT_DIR}/opcode/functions.h")

add_custom_command(
    OUTPUT ${FUNCTIONS_CC} ${FUNCTIONS_H}
    COMMAND 
        ${Python3_EXECUTABLE} "${SCRIPT_SOURCE_DIR}/gen_functions.py"
            --output_dir "${GENSRC_OUTPUT_DIR}/opcode"
            --build_dir "${CMAKE_SOURCE_DIR}/../gensrc/build/python"
    DEPENDS 
        "${SCRIPT_SOURCE_DIR}/gen_functions.py"
    COMMENT "Generating functions.cc and functions.h from Python"
)
list(APPEND GENSRC_GENERATED_FILES ${FUNCTIONS_CC} ${FUNCTIONS_H})

# Also generate Opcodes_types from thrift
set(OPCODES_TYPES_CC "${GENSRC_OUTPUT_DIR}/Opcodes_types.cpp")
set(OPCODES_TYPES_H "${GENSRC_OUTPUT_DIR}/Opcodes_types.h")
if(EXISTS "${THRIFT_SOURCE_DIR}/Opcodes.thrift")
    add_custom_command(
        OUTPUT ${OPCODES_TYPES_CC} ${OPCODES_TYPES_H}
        COMMAND ${THRIFT_EXECUTABLE}
            ARGS -I ${THRIFT_SOURCE_DIR}
                 -I "${CMAKE_SOURCE_DIR}/../gensrc/build/thrift"
                 --gen cpp:moveable_types,no_skeleton
                 -out ${GENSRC_OUTPUT_DIR}
                 --allow-64bit-consts
                 -strict
                 "${THRIFT_SOURCE_DIR}/Opcodes.thrift"
        DEPENDS "${THRIFT_SOURCE_DIR}/Opcodes.thrift" ${THRIFT_EXECUTABLE}
        COMMENT "Generating Opcodes_types from thrift"
        VERBATIM
    )
    list(APPEND GENSRC_GENERATED_FILES ${OPCODES_TYPES_CC} ${OPCODES_TYPES_H})
endif()

# Create a custom target that depends on all generated files
# Using ALL to ensure gensrc is built by default
add_custom_target(gensrc ALL
    DEPENDS ${GENSRC_GENERATED_FILES}
    COMMENT "Generating source code from proto and thrift files"
)

message(STATUS "GENSRC: Output directory: ${GENSRC_OUTPUT_DIR}")
message(STATUS "GENSRC: Generated proto files: ${GENSRC_PROTO_CC_FILES}")
message(STATUS "GENSRC: Generated thrift files: ${GENSRC_THRIFT_CC_FILES}")
