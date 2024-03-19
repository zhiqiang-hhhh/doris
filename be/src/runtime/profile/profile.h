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

#include <gen_cpp/RuntimeProfile_types.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "util/runtime_profile.h"

namespace doris::profile {
// // for pipeline
// struct InstanceProfile {
//     TUniqueId instance_id;
//     TRuntimeProfileTree profile;
// };

// using InstanceProfilePtr = std::shared_ptr<InstanceProfile>;

// struct QueryProfile {
//     bool finished = false;
//     TUniqueId query_id;
//     std::vector<InstanceProfilePtr> instance_profiles;
// };

// using QueryProfilePtr = std::shared_ptr<QueryProfile>;

// struct ProfileReportQueueEntry {
//     TNetworkAddress coordinator_addr;
//     QueryProfilePtr query_profile;
// };

// using ProfileReportQueueEntryPtr = std::shared_ptr<ProfileReportQueueEntry>;

using TRuntimeProfilePtr = std::shared_ptr<TRuntimeProfileTree>;

struct PipelineProfileX {
    int32_t fragment_id;
    bool finished;
    TRuntimeProfilePtr pipeline_profile;
};

using PipelineProfilePtrX = std::shared_ptr<PipelineProfileX>;
using FragmentProfileX = std::vector<PipelineProfilePtrX>;

struct QueryProfileX {
    TUniqueId query_id;
    bool finished = false;
    std::map<int, FragmentProfileX> fid_to_profile;
};

} // namespace doris::profile