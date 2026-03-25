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

# Build version information for Doris
# These values are used in gensrc generation

# Get git information
find_package(Git QUIET)

if(Git_FOUND)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} describe --tags --always --dirty
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE DORIS_BUILD_COMMIT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
else()
    set(DORIS_BUILD_COMMIT_HASH "unknown")
endif()

# Default version (can be overridden)
set(DORIS_BUILD_VERSION "1.0.0")
set(DORIS_BUILD_VERSION_MAJOR 1)
set(DORIS_BUILD_VERSION_MINOR 0)
set(DORIS_BUILD_VERSION_PATCH 0)

# Build time
string(TIMESTAMP DORIS_BUILD_TIME "%Y-%m-%d %H:%M:%S" UTC)

message(STATUS "Doris build version: ${DORIS_BUILD_VERSION}")
message(STATUS "Doris commit hash: ${DORIS_BUILD_COMMIT_HASH}")
message(STATUS "Doris build time: ${DORIS_BUILD_TIME}")
