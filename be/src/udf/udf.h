// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/udf/udf.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "vec/common/arena.h"

namespace doris {

struct ColumnPtrWrapper;
struct StringRef;
class RuntimeState;

namespace vectorized {
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
} // namespace vectorized

// The FunctionContext is passed to every UDF/UDA and is the interface for the UDF to the
// rest of the system. It contains APIs to examine the system state, report errors
// and manage memory.
class FunctionContext {
public:
    enum FunctionStateScope {
        /// Indicates that the function state for this FunctionContext's UDF is shared across
        /// the plan fragment (a query is divided into multiple plan fragments, each of which
        /// is responsible for a part of the query execution). Within the plan fragment, there
        /// may be multiple instances of the UDF executing concurrently with multiple
        /// FunctionContexts sharing this state, meaning that the state must be
        /// thread-safe. The Prepare() function for the UDF may be called with this scope
        /// concurrently on a single host if the UDF will be evaluated in multiple plan
        /// fragments on that host. In general, read-only state that doesn't need to be
        /// recomputed for every UDF call should be fragment-local.
        /// TODO: not yet implemented
        FRAGMENT_LOCAL,

        /// Indicates that the function state is local to the execution thread. This state
        /// does not need to be thread-safe. However, this state will be initialized (via the
        /// Prepare() function) once for every execution thread, so fragment-local state
        /// should be used when possible for better performance. In general, inexpensive
        /// shared state that is written to by the UDF (e.g. scratch space) should be
        /// thread-local.
        THREAD_LOCAL,
    };

    static std::unique_ptr<doris::FunctionContext> create_context(
            RuntimeState* state, const vectorized::DataTypePtr& return_type,
            const std::vector<vectorized::DataTypePtr>& arg_types);

    /// Returns a new FunctionContext with the same constant args, fragment-local state, and
    /// debug flag as this FunctionContext. The caller is responsible for calling delete on
    /// it.
    std::unique_ptr<doris::FunctionContext> clone();

    void set_constant_cols(const std::vector<std::shared_ptr<doris::ColumnPtrWrapper>>& cols);

    RuntimeState* state() { return _state; }

    void set_dict_function(const TDictFunction& dict_function) { _dict_function = dict_function; }

    std::optional<TDictFunction>& dict_function() { return _dict_function; };
    bool check_overflow_for_decimal() const { return _check_overflow_for_decimal; }

    bool enable_strict_mode() const { return _enable_strict_mode; }

    bool set_check_overflow_for_decimal(bool check_overflow_for_decimal) {
        return _check_overflow_for_decimal = check_overflow_for_decimal;
    }

    bool set_enable_strict_mode(bool enable_strict_mode) {
        return _enable_strict_mode = enable_strict_mode;
    }

    void set_string_as_jsonb_string(bool string_as_jsonb_string) {
        _string_as_jsonb_string = string_as_jsonb_string;
    }

    void set_jsonb_string_as_string(bool jsonb_string_as_string) {
        _jsonb_string_as_string = jsonb_string_as_string;
    }

    void set_udf_execute_timer(RuntimeProfile::Counter* udf_execute_timer) {
        _udf_execute_timer = udf_execute_timer;
    }

    RuntimeProfile::Counter* get_udf_execute_timer() { return _udf_execute_timer; }

    // Cast flag, when enable string_as_jsonb_string, string casting to jsonb will not parse string
    // instead just insert a string literal
    bool string_as_jsonb_string() const { return _string_as_jsonb_string; }

    // Cast flag, when enable jsonb_string_as_string, jsonb string casting to string will not parse string
    // instead just insert a string literal
    bool jsonb_string_as_string() const { return _jsonb_string_as_string; }

    // Sets an error for this UDF. If this is called, this will trigger the
    // query to fail.
    // Note: when you set error for the UDFs used in Data Load, you should
    // ensure the function return value is null.
    void set_error(const char* error_msg);

    // Adds a warning that is returned to the user. This can include things like
    // overflow or other recoverable error conditions.
    // Warnings are capped at a maximum number. Returns true if the warning was
    // added and false if it was ignored due to the cap.
    bool add_warning(const char* warning_msg);

    /// Methods for maintaining state across UDF/UDA function calls. SetFunctionState() can
    /// be used to store a pointer that can then be retrieved via GetFunctionState(). If
    /// GetFunctionState() is called when no pointer is set, it will return
    /// nullptr. SetFunctionState() does not take ownership of 'ptr'; it is up to the UDF/UDA
    /// to clean up any function state if necessary.
    void set_function_state(FunctionStateScope scope, std::shared_ptr<void> ptr);

    void* get_function_state(FunctionStateScope scope) const;

    // Returns the return type information of this function. For UDAs, this is the final
    // return type of the UDA (e.g., the type returned by the finalize function).
    const vectorized::DataTypePtr get_return_type() const;

    // Returns the number of arguments to this function (not including the FunctionContext*
    // argument).
    int get_num_args() const;

    // Returns the type information for the arg_idx-th argument (0-indexed, not including
    // the FunctionContext* argument). Returns nullptr if arg_idx is invalid.
    const vectorized::DataTypePtr get_arg_type(int arg_idx) const;

    // Returns true if the arg_idx-th input argument (0 indexed, not including the
    // FunctionContext* argument) is a constant (e.g. 5, "string", 1 + 1).
    bool is_col_constant(int arg_idx) const;

    // Returns a pointer to the value of the arg_idx-th input argument (0 indexed, not
    // including the FunctionContext* argument). Returns nullptr if the argument is not
    // constant. This function can be used to obtain user-specified constants in a UDF's
    // Init() or Close() functions.
    doris::ColumnPtrWrapper* get_constant_col(int arg_idx) const;

    // Creates a StringRef, which memory is available when this function context is used next time
    StringRef create_temp_string_val(int64_t len);

    ~FunctionContext() = default;

    vectorized::Arena& get_arena() { return arena; }

private:
    FunctionContext() = default;

    // Disable copy ctor and assignment operator
    FunctionContext(const FunctionContext& other);

    FunctionContext& operator=(const FunctionContext& other);

    // We use the query's runtime state to report errors and warnings. nullptr for test
    // contexts.
    RuntimeState* _state = nullptr;

    // Empty if there's no error
    std::string _error_msg;

    // The number of warnings reported.
    int64_t _num_warnings;

    /// The function state accessed via FunctionContext::Get/SetFunctionState()
    std::shared_ptr<void> _thread_local_fn_state;
    std::shared_ptr<void> _fragment_local_fn_state;

    // Type descriptor for the return type of the function.
    vectorized::DataTypePtr _return_type;

    // Type descriptors for each argument of the function.
    std::vector<vectorized::DataTypePtr> _arg_types;

    std::vector<std::shared_ptr<doris::ColumnPtrWrapper>> _constant_cols;

    //udf execute timer
    RuntimeProfile::Counter* _udf_execute_timer = nullptr;
    bool _check_overflow_for_decimal = false;
    bool _enable_strict_mode = false;

    bool _string_as_jsonb_string = false;
    bool _jsonb_string_as_string = false;

    std::string _string_result;

    vectorized::Arena arena;

    std::optional<TDictFunction> _dict_function;
};

using doris::FunctionContext;
} // namespace doris
