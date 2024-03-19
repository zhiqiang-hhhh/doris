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

#pragma once

#include <gen_cpp/Data_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>

#include <memory>
#include <shared_mutex>
#include <string>

#include "profile/profile.h"
#include "runtime/query_statistics.h"
#include "runtime/workload_management/workload_condition.h"
#include "util/time.h"

namespace doris {

namespace vectorized {
class Block;
} // namespace vectorized

class QueryStatisticsCtx {
public:
    QueryStatisticsCtx(TNetworkAddress fe_addr) : _fe_addr(fe_addr) {
        this->_is_query_finished = false;
        this->_wg_id = -1;
        this->_query_start_time = MonotonicMillis();
    }
    ~QueryStatisticsCtx() = default;

    void collect_query_statistics(TQueryStatistics* tq_s);

public:
    std::vector<std::shared_ptr<QueryStatistics>> _qs_list;
    bool _is_query_finished;
    TNetworkAddress _fe_addr;
    int64_t _query_finish_time;
    int64_t _wg_id;
    int64_t _query_start_time;
};

class RuntimeQueryStatiticsMgr {
public:
    RuntimeQueryStatiticsMgr() = default;
    ~RuntimeQueryStatiticsMgr() = default;

    void register_query_statistics(std::string query_id, std::shared_ptr<QueryStatistics> qs_ptr,
                                   TNetworkAddress fe_addr);

    void report_runtime_query_statistics();

    void set_query_finished(std::string query_id);

    std::shared_ptr<QueryStatistics> get_runtime_query_statistics(std::string query_id);

    void set_workload_group_id(std::string query_id, int64_t wg_id);

    // used for workload scheduler policy
    void get_metric_map(std::string query_id,
                        std::map<WorkloadMetricType, std::string>& metric_map);

    // used for backend_active_tasks
    void get_active_be_tasks_block(vectorized::Block* block);

    void register_fragment_profile_x(const TUniqueId& query_id, int32_t fragment_id,
                                     const TNetworkAddress& const_addr,
                                     const profile::FragmentProfileX& profile);

    void report_query_profiles_x();

private:
    std::shared_mutex _qs_ctx_map_lock;
    std::map<std::string, std::unique_ptr<QueryStatisticsCtx>> _query_statistics_ctx_map;

    struct CoorAddrFragmentProfile {
        TNetworkAddress coordinator_addr;
        TUniqueId query_id;
        profile::FragmentProfileX fragment_profile;
    };

    std::shared_mutex _query_profile_map_lock;
    using CoorAddrFragmentProfilePtr =
            std::shared_ptr<CoorAddrFragmentProfile>;

    using QueryIdFragmentId = std::pair<std::string, int32_t>;
    // <query_id, fragment_id> -> {coordinator_addr, query_id, profile}
    std::map<QueryIdFragmentId, CoorAddrFragmentProfilePtr>
            _fragment_profile_map_x;
};

} // namespace doris