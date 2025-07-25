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

#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <stdint.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "agent/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "io/fs/path.h"
#include "olap/base_tablet.h"
#include "olap/cold_data_compaction.h"
#include "olap/compaction_permit_limiter.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/segcompaction.h"
#include "olap/schema_change.h"
#include "olap/single_replica_compaction.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/task/index_builder.h"
#include "runtime/client_cache.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "util/countdown_latch.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "util/work_thread_pool.hpp"

using std::string;

namespace doris {
#include "common/compile_check_begin.h"
using io::Path;

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

static const uint64_t DEFAULT_SEED = 104729;
static const uint64_t MOD_PRIME = 7652413;

CompactionSubmitRegistry::CompactionSubmitRegistry(CompactionSubmitRegistry&& r) {
    std::swap(_tablet_submitted_cumu_compaction, r._tablet_submitted_cumu_compaction);
    std::swap(_tablet_submitted_base_compaction, r._tablet_submitted_base_compaction);
    std::swap(_tablet_submitted_full_compaction, r._tablet_submitted_full_compaction);
}

CompactionSubmitRegistry CompactionSubmitRegistry::create_snapshot() {
    // full compaction is not engaged in this method
    std::unique_lock<std::mutex> l(_tablet_submitted_compaction_mutex);
    CompactionSubmitRegistry registry;
    registry._tablet_submitted_base_compaction = _tablet_submitted_base_compaction;
    registry._tablet_submitted_cumu_compaction = _tablet_submitted_cumu_compaction;
    return registry;
}

void CompactionSubmitRegistry::reset(const std::vector<DataDir*>& stores) {
    // full compaction is not engaged in this method
    for (const auto& store : stores) {
        _tablet_submitted_cumu_compaction[store] = {};
        _tablet_submitted_base_compaction[store] = {};
    }
}

uint32_t CompactionSubmitRegistry::count_executing_compaction(DataDir* dir,
                                                              CompactionType compaction_type) {
    // non-lock, used in snapshot
    const auto& compaction_tasks = _get_tablet_set(dir, compaction_type);
    return cast_set<uint32_t>(std::count_if(
            compaction_tasks.begin(), compaction_tasks.end(),
            [](const auto& task) { return task->compaction_stage == CompactionStage::EXECUTING; }));
}

uint32_t CompactionSubmitRegistry::count_executing_cumu_and_base(DataDir* dir) {
    // non-lock, used in snapshot
    return count_executing_compaction(dir, CompactionType::BASE_COMPACTION) +
           count_executing_compaction(dir, CompactionType::CUMULATIVE_COMPACTION);
}

bool CompactionSubmitRegistry::has_compaction_task(DataDir* dir, CompactionType compaction_type) {
    // non-lock, used in snapshot
    return !_get_tablet_set(dir, compaction_type).empty();
}

std::vector<TabletSharedPtr> CompactionSubmitRegistry::pick_topn_tablets_for_compaction(
        TabletManager* tablet_mgr, DataDir* data_dir, CompactionType compaction_type,
        const CumuCompactionPolicyTable& cumu_compaction_policies, uint32_t* disk_max_score) {
    // non-lock, used in snapshot
    return tablet_mgr->find_best_tablets_to_compaction(compaction_type, data_dir,
                                                       _get_tablet_set(data_dir, compaction_type),
                                                       disk_max_score, cumu_compaction_policies);
}

bool CompactionSubmitRegistry::insert(TabletSharedPtr tablet, CompactionType compaction_type) {
    std::unique_lock<std::mutex> l(_tablet_submitted_compaction_mutex);
    auto& tablet_set = _get_tablet_set(tablet->data_dir(), compaction_type);
    bool already_exist = !(tablet_set.insert(tablet).second);
    return already_exist;
}

void CompactionSubmitRegistry::remove(TabletSharedPtr tablet, CompactionType compaction_type,
                                      std::function<void()> wakeup_cb) {
    std::unique_lock<std::mutex> l(_tablet_submitted_compaction_mutex);
    auto& tablet_set = _get_tablet_set(tablet->data_dir(), compaction_type);
    size_t removed = tablet_set.erase(tablet);
    if (removed == 1) {
        wakeup_cb();
    }
}

CompactionSubmitRegistry::TabletSet& CompactionSubmitRegistry::_get_tablet_set(
        DataDir* dir, CompactionType compaction_type) {
    switch (compaction_type) {
    case CompactionType::BASE_COMPACTION:
        return _tablet_submitted_base_compaction[dir];
    case CompactionType::CUMULATIVE_COMPACTION:
        return _tablet_submitted_cumu_compaction[dir];
    case CompactionType::FULL_COMPACTION:
        return _tablet_submitted_full_compaction[dir];
    default:
        CHECK(false) << "invalid compaction type";
    }
}

static int32_t get_cumu_compaction_threads_num(size_t data_dirs_num) {
    int32_t threads_num = config::max_cumu_compaction_threads;
    if (threads_num == -1) {
        int32_t num_cores = doris::CpuInfo::num_cores();
        threads_num = std::max(cast_set<int32_t>(data_dirs_num), num_cores / 6);
    }
    threads_num = threads_num <= 0 ? 1 : threads_num;
    return threads_num;
}

static int32_t get_base_compaction_threads_num(size_t data_dirs_num) {
    int32_t threads_num = config::max_base_compaction_threads;
    if (threads_num == -1) {
        threads_num = cast_set<int32_t>(data_dirs_num);
    }
    threads_num = threads_num <= 0 ? 1 : threads_num;
    return threads_num;
}

static int32_t get_single_replica_compaction_threads_num(size_t data_dirs_num) {
    int32_t threads_num = config::max_single_replica_compaction_threads;
    if (threads_num == -1) {
        threads_num = cast_set<int32_t>(data_dirs_num);
    }
    threads_num = threads_num <= 0 ? 1 : threads_num;
    return threads_num;
}

Status StorageEngine::start_bg_threads(std::shared_ptr<WorkloadGroup> wg_sptr) {
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "unused_rowset_monitor_thread",
            [this]() { this->_unused_rowset_monitor_thread_callback(); },
            &_unused_rowset_monitor_thread));
    LOG(INFO) << "unused rowset monitor thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "evict_querying_rowset_thread",
            [this]() { this->_evict_quring_rowset_thread_callback(); },
            &_evict_quering_rowset_thread));
    LOG(INFO) << "evict quering thread started";

    // start thread for monitoring the snapshot and trash folder
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "garbage_sweeper_thread",
            [this]() { this->_garbage_sweeper_thread_callback(); }, &_garbage_sweeper_thread));
    LOG(INFO) << "garbage sweeper thread started";

    // start thread for monitoring the tablet with io error
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "disk_stat_monitor_thread",
            [this]() { this->_disk_stat_monitor_thread_callback(); }, &_disk_stat_monitor_thread));
    LOG(INFO) << "disk stat monitor thread started";

    // convert store map to vector
    std::vector<DataDir*> data_dirs = get_stores();

    auto base_compaction_threads = get_base_compaction_threads_num(data_dirs.size());
    auto cumu_compaction_threads = get_cumu_compaction_threads_num(data_dirs.size());
    auto single_replica_compaction_threads =
            get_single_replica_compaction_threads_num(data_dirs.size());

    RETURN_IF_ERROR(ThreadPoolBuilder("BaseCompactionTaskThreadPool")
                            .set_min_threads(base_compaction_threads)
                            .set_max_threads(base_compaction_threads)
                            .build(&_base_compaction_thread_pool));
    RETURN_IF_ERROR(ThreadPoolBuilder("CumuCompactionTaskThreadPool")
                            .set_min_threads(cumu_compaction_threads)
                            .set_max_threads(cumu_compaction_threads)
                            .build(&_cumu_compaction_thread_pool));
    RETURN_IF_ERROR(ThreadPoolBuilder("SingleReplicaCompactionTaskThreadPool")
                            .set_min_threads(single_replica_compaction_threads)
                            .set_max_threads(single_replica_compaction_threads)
                            .build(&_single_replica_compaction_thread_pool));

    if (config::enable_segcompaction) {
        RETURN_IF_ERROR(ThreadPoolBuilder("SegCompactionTaskThreadPool")
                                .set_min_threads(config::segcompaction_num_threads)
                                .set_max_threads(config::segcompaction_num_threads)
                                .build(&_seg_compaction_thread_pool));
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("ColdDataCompactionTaskThreadPool")
                            .set_min_threads(config::cold_data_compaction_thread_num)
                            .set_max_threads(config::cold_data_compaction_thread_num)
                            .build(&_cold_data_compaction_thread_pool));

    // compaction tasks producer thread
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "compaction_tasks_producer_thread",
            [this]() { this->_compaction_tasks_producer_callback(); },
            &_compaction_tasks_producer_thread));
    LOG(INFO) << "compaction tasks producer thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "_update_replica_infos_thread",
            [this]() { this->_update_replica_infos_callback(); }, &_update_replica_infos_thread));
    LOG(INFO) << "tablet replicas info update thread started";

    int32_t max_checkpoint_thread_num = config::max_meta_checkpoint_threads;
    if (max_checkpoint_thread_num < 0) {
        max_checkpoint_thread_num = cast_set<int32_t>(data_dirs.size());
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("TabletMetaCheckpointTaskThreadPool")
                            .set_max_threads(max_checkpoint_thread_num)
                            .build(&_tablet_meta_checkpoint_thread_pool));

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "tablet_checkpoint_tasks_producer_thread",
            [this, data_dirs]() { this->_tablet_checkpoint_callback(data_dirs); },
            &_tablet_checkpoint_tasks_producer_thread));
    LOG(INFO) << "tablet checkpoint tasks producer thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "tablet_path_check_thread",
            [this]() { this->_tablet_path_check_callback(); }, &_tablet_path_check_thread));
    LOG(INFO) << "tablet path check thread started";

    // path scan and gc thread
    if (config::path_gc_check) {
        for (auto data_dir : get_stores()) {
            scoped_refptr<Thread> path_gc_thread;
            RETURN_IF_ERROR(Thread::create(
                    "StorageEngine", "path_gc_thread",
                    [this, data_dir]() { this->_path_gc_thread_callback(data_dir); },
                    &path_gc_thread));
            _path_gc_threads.emplace_back(path_gc_thread);
        }
        LOG(INFO) << "path gc threads started. number:" << get_stores().size();
    }

    RETURN_IF_ERROR(ThreadPoolBuilder("CooldownTaskThreadPool")
                            .set_min_threads(config::cooldown_thread_num)
                            .set_max_threads(config::cooldown_thread_num)
                            .build(&_cooldown_thread_pool));
    LOG(INFO) << "cooldown thread pool started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "cooldown_tasks_producer_thread",
            [this]() { this->_cooldown_tasks_producer_callback(); },
            &_cooldown_tasks_producer_thread));
    LOG(INFO) << "cooldown tasks producer thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "remove_unused_remote_files_thread",
            [this]() { this->_remove_unused_remote_files_callback(); },
            &_remove_unused_remote_files_thread));
    LOG(INFO) << "remove unused remote files thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "cold_data_compaction_producer_thread",
            [this]() { this->_cold_data_compaction_producer_callback(); },
            &_cold_data_compaction_producer_thread));
    LOG(INFO) << "cold data compaction producer thread started";

    // add tablet publish version thread pool
    RETURN_IF_ERROR(ThreadPoolBuilder("TabletPublishTxnThreadPool")
                            .set_min_threads(config::tablet_publish_txn_max_thread)
                            .set_max_threads(config::tablet_publish_txn_max_thread)
                            .build(&_tablet_publish_txn_thread_pool));

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "async_publish_version_thread",
            [this]() { this->_async_publish_callback(); }, &_async_publish_thread));
    LOG(INFO) << "async publish thread started";

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "check_tablet_delete_bitmap_score_thread",
            [this]() { this->_check_tablet_delete_bitmap_score_callback(); },
            &_check_delete_bitmap_score_thread));
    LOG(INFO) << "check tablet delete bitmap score thread started";

    LOG(INFO) << "all storage engine's background threads are started.";
    return Status::OK();
}

void StorageEngine::_garbage_sweeper_thread_callback() {
    uint32_t max_interval = config::max_garbage_sweep_interval;
    uint32_t min_interval = config::min_garbage_sweep_interval;

    if (max_interval < min_interval || min_interval <= 0) {
        LOG(WARNING) << "garbage sweep interval config is illegal: [max=" << max_interval
                     << " min=" << min_interval << "].";
        min_interval = 1;
        max_interval = max_interval >= min_interval ? max_interval : min_interval;
        LOG(INFO) << "force reset garbage sweep interval. "
                  << "max_interval=" << max_interval << ", min_interval=" << min_interval;
    }

    const double pi = M_PI;
    double usage = 1.0;
    // After the program starts, the first round of cleaning starts after min_interval.
    uint32_t curr_interval = min_interval;
    do {
        // Function properties:
        // when usage < 0.6,          ratio close to 1.(interval close to max_interval)
        // when usage at [0.6, 0.75], ratio is rapidly decreasing from 0.87 to 0.27.
        // when usage > 0.75,         ratio is slowly decreasing.
        // when usage > 0.8,          ratio close to min_interval.
        // when usage = 0.88,         ratio is approximately 0.0057.
        double ratio = (1.1 * (pi / 2 - std::atan(usage * 100 / 5 - 14)) - 0.28) / pi;
        ratio = ratio > 0 ? ratio : 0;
        curr_interval = uint32_t(max_interval * ratio);
        curr_interval = std::max(curr_interval, min_interval);
        curr_interval = std::min(curr_interval, max_interval);

        // start clean trash and update usage.
        Status res = start_trash_sweep(&usage);
        if (res.ok() && _need_clean_trash.exchange(false, std::memory_order_relaxed)) {
            res = start_trash_sweep(&usage, true);
        }

        if (!res.ok()) {
            LOG(WARNING) << "one or more errors occur when sweep trash."
                         << "see previous message for detail. err code=" << res;
            // do nothing. continue next loop.
        }
        LOG(INFO) << "trash thread check usage=" << usage << " ratio=" << ratio
                  << " curr_interval=" << curr_interval;
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(curr_interval)));
}

void StorageEngine::_disk_stat_monitor_thread_callback() {
    int32_t interval = config::disk_stat_monitor_interval;
    do {
        _start_disk_stat_monitor();

        interval = config::disk_stat_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "disk_stat_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

void StorageEngine::_unused_rowset_monitor_thread_callback() {
    int32_t interval = config::unused_rowset_monitor_interval;
    do {
        start_delete_unused_rowset();

        interval = config::unused_rowset_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "unused_rowset_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

int32_t StorageEngine::_auto_get_interval_by_disk_capacity(DataDir* data_dir) {
    double disk_used = data_dir->get_usage(0);
    double remain_used = 1 - disk_used;
    DCHECK(remain_used >= 0 && remain_used <= 1);
    DCHECK(config::path_gc_check_interval_second >= 0);
    int32_t ret = 0;
    if (remain_used > 0.9) {
        // if config::path_gc_check_interval_second == 24h
        ret = config::path_gc_check_interval_second;
    } else if (remain_used > 0.7) {
        // 12h
        ret = config::path_gc_check_interval_second / 2;
    } else if (remain_used > 0.5) {
        // 6h
        ret = config::path_gc_check_interval_second / 4;
    } else if (remain_used > 0.3) {
        // 4h
        ret = config::path_gc_check_interval_second / 6;
    } else {
        // 3h
        ret = config::path_gc_check_interval_second / 8;
    }
    return ret;
}

void StorageEngine::_path_gc_thread_callback(DataDir* data_dir) {
    LOG(INFO) << "try to start path gc thread!";
    time_t last_exec_time = 0;
    do {
        time_t current_time = time(nullptr);

        int32_t interval = _auto_get_interval_by_disk_capacity(data_dir);
        DBUG_EXECUTE_IF("_path_gc_thread_callback.interval.eq.1ms", {
            LOG(INFO) << "debug point change interval eq 1ms";
            interval = 1;
            while (DebugPoints::instance()->is_enable("_path_gc_thread_callback.always.do")) {
                data_dir->perform_path_gc();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << " will be forced set to half hour";
            interval = 1800; // 0.5 hour
        }
        if (current_time - last_exec_time >= interval) {
            LOG(INFO) << "try to perform path gc! disk remain [" << 1 - data_dir->get_usage(0)
                      << "] internal [" << interval << "]";
            data_dir->perform_path_gc();
            last_exec_time = time(nullptr);
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(5)));
    LOG(INFO) << "stop path gc thread!";
}

void StorageEngine::_tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs) {
    int64_t interval = config::generate_tablet_meta_checkpoint_tasks_interval_secs;
    do {
        for (auto data_dir : data_dirs) {
            LOG(INFO) << "begin to produce tablet meta checkpoint tasks, data_dir="
                      << data_dir->path();
            auto st = _tablet_meta_checkpoint_thread_pool->submit_func(
                    [data_dir, this]() { _tablet_manager->do_tablet_meta_checkpoint(data_dir); });
            if (!st.ok()) {
                LOG(WARNING) << "submit tablet checkpoint tasks failed.";
            }
        }
        interval = config::generate_tablet_meta_checkpoint_tasks_interval_secs;
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

void StorageEngine::_tablet_path_check_callback() {
    struct TabletIdComparator {
        bool operator()(Tablet* a, Tablet* b) { return a->tablet_id() < b->tablet_id(); }
    };

    using TabletQueue = std::priority_queue<Tablet*, std::vector<Tablet*>, TabletIdComparator>;

    int64_t interval = config::tablet_path_check_interval_seconds;
    if (interval <= 0) {
        return;
    }

    int64_t last_tablet_id = 0;
    do {
        int32_t batch_size = config::tablet_path_check_batch_size;
        if (batch_size <= 0) {
            if (_stop_background_threads_latch.wait_for(std::chrono::seconds(interval))) {
                break;
            }
            continue;
        }

        LOG(INFO) << "start to check tablet path";

        auto all_tablets = _tablet_manager->get_all_tablet(
                [](Tablet* t) { return t->is_used() && t->tablet_state() == TABLET_RUNNING; });

        TabletQueue big_id_tablets;
        TabletQueue small_id_tablets;
        for (auto tablet : all_tablets) {
            auto tablet_id = tablet->tablet_id();
            TabletQueue* belong_tablets = nullptr;
            if (tablet_id > last_tablet_id) {
                if (big_id_tablets.size() < batch_size ||
                    big_id_tablets.top()->tablet_id() > tablet_id) {
                    belong_tablets = &big_id_tablets;
                }
            } else if (big_id_tablets.size() < batch_size) {
                if (small_id_tablets.size() < batch_size ||
                    small_id_tablets.top()->tablet_id() > tablet_id) {
                    belong_tablets = &small_id_tablets;
                }
            }
            if (belong_tablets != nullptr) {
                belong_tablets->push(tablet.get());
                if (belong_tablets->size() > batch_size) {
                    belong_tablets->pop();
                }
            }
        }

        int32_t need_small_id_tablet_size =
                batch_size - static_cast<int32_t>(big_id_tablets.size());

        if (!big_id_tablets.empty()) {
            last_tablet_id = big_id_tablets.top()->tablet_id();
        }
        while (!big_id_tablets.empty()) {
            big_id_tablets.top()->check_tablet_path_exists();
            big_id_tablets.pop();
        }

        if (!small_id_tablets.empty() && need_small_id_tablet_size > 0) {
            while (static_cast<int32_t>(small_id_tablets.size()) > need_small_id_tablet_size) {
                small_id_tablets.pop();
            }

            last_tablet_id = small_id_tablets.top()->tablet_id();
            while (!small_id_tablets.empty()) {
                small_id_tablets.top()->check_tablet_path_exists();
                small_id_tablets.pop();
            }
        }

    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

void StorageEngine::_adjust_compaction_thread_num() {
    auto base_compaction_threads_num = get_base_compaction_threads_num(_store_map.size());
    if (_base_compaction_thread_pool->max_threads() != base_compaction_threads_num) {
        int old_max_threads = _base_compaction_thread_pool->max_threads();
        Status status = _base_compaction_thread_pool->set_max_threads(base_compaction_threads_num);
        if (status.ok()) {
            VLOG_NOTICE << "update base compaction thread pool max_threads from " << old_max_threads
                        << " to " << base_compaction_threads_num;
        }
    }
    if (_base_compaction_thread_pool->min_threads() != base_compaction_threads_num) {
        int old_min_threads = _base_compaction_thread_pool->min_threads();
        Status status = _base_compaction_thread_pool->set_min_threads(base_compaction_threads_num);
        if (status.ok()) {
            VLOG_NOTICE << "update base compaction thread pool min_threads from " << old_min_threads
                        << " to " << base_compaction_threads_num;
        }
    }

    auto cumu_compaction_threads_num = get_cumu_compaction_threads_num(_store_map.size());
    if (_cumu_compaction_thread_pool->max_threads() != cumu_compaction_threads_num) {
        int old_max_threads = _cumu_compaction_thread_pool->max_threads();
        Status status = _cumu_compaction_thread_pool->set_max_threads(cumu_compaction_threads_num);
        if (status.ok()) {
            VLOG_NOTICE << "update cumu compaction thread pool max_threads from " << old_max_threads
                        << " to " << cumu_compaction_threads_num;
        }
    }
    if (_cumu_compaction_thread_pool->min_threads() != cumu_compaction_threads_num) {
        int old_min_threads = _cumu_compaction_thread_pool->min_threads();
        Status status = _cumu_compaction_thread_pool->set_min_threads(cumu_compaction_threads_num);
        if (status.ok()) {
            VLOG_NOTICE << "update cumu compaction thread pool min_threads from " << old_min_threads
                        << " to " << cumu_compaction_threads_num;
        }
    }

    auto single_replica_compaction_threads_num =
            get_single_replica_compaction_threads_num(_store_map.size());
    if (_single_replica_compaction_thread_pool->max_threads() !=
        single_replica_compaction_threads_num) {
        int old_max_threads = _single_replica_compaction_thread_pool->max_threads();
        Status status = _single_replica_compaction_thread_pool->set_max_threads(
                single_replica_compaction_threads_num);
        if (status.ok()) {
            VLOG_NOTICE << "update single replica compaction thread pool max_threads from "
                        << old_max_threads << " to " << single_replica_compaction_threads_num;
        }
    }
    if (_single_replica_compaction_thread_pool->min_threads() !=
        single_replica_compaction_threads_num) {
        int old_min_threads = _single_replica_compaction_thread_pool->min_threads();
        Status status = _single_replica_compaction_thread_pool->set_min_threads(
                single_replica_compaction_threads_num);
        if (status.ok()) {
            VLOG_NOTICE << "update single replica compaction thread pool min_threads from "
                        << old_min_threads << " to " << single_replica_compaction_threads_num;
        }
    }
}

void StorageEngine::_compaction_tasks_producer_callback() {
    LOG(INFO) << "try to start compaction producer process!";

    std::vector<DataDir*> data_dirs = get_stores();
    _compaction_submit_registry.reset(data_dirs);

    int round = 0;
    CompactionType compaction_type;

    // Used to record the time when the score metric was last updated.
    // The update of the score metric is accompanied by the logic of selecting the tablet.
    // If there is no slot available, the logic of selecting the tablet will be terminated,
    // which causes the score metric update to be terminated.
    // In order to avoid this situation, we need to update the score regularly.
    int64_t last_cumulative_score_update_time = 0;
    int64_t last_base_score_update_time = 0;
    static const int64_t check_score_interval_ms = 5000; // 5 secs

    int64_t interval = config::generate_compaction_tasks_interval_ms;
    do {
        int64_t cur_time = UnixMillis();
        if (!config::disable_auto_compaction &&
            (!config::enable_compaction_pause_on_high_memory ||
             !GlobalMemoryArbitrator::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE))) {
            _adjust_compaction_thread_num();

            bool check_score = false;
            if (round < config::cumulative_compaction_rounds_for_each_base_compaction_round) {
                compaction_type = CompactionType::CUMULATIVE_COMPACTION;
                round++;
                if (cur_time - last_cumulative_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_cumulative_score_update_time = cur_time;
                }
            } else {
                compaction_type = CompactionType::BASE_COMPACTION;
                round = 0;
                if (cur_time - last_base_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_base_score_update_time = cur_time;
                }
            }
            std::vector<TabletSharedPtr> tablets_compaction =
                    _generate_compaction_tasks(compaction_type, data_dirs, check_score);
            if (tablets_compaction.size() == 0) {
                std::unique_lock<std::mutex> lock(_compaction_producer_sleep_mutex);
                _wakeup_producer_flag = 0;
                // It is necessary to wake up the thread on timeout to prevent deadlock
                // in case of no running compaction task.
                _compaction_producer_sleep_cv.wait_for(
                        lock, std::chrono::milliseconds(2000),
                        [this] { return _wakeup_producer_flag == 1; });
                continue;
            }

            for (const auto& tablet : tablets_compaction) {
                if (compaction_type == CompactionType::BASE_COMPACTION) {
                    tablet->set_last_base_compaction_schedule_time(UnixMillis());
                } else if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                    tablet->set_last_cumu_compaction_schedule_time(UnixMillis());
                } else if (compaction_type == CompactionType::FULL_COMPACTION) {
                    tablet->set_last_full_compaction_schedule_time(UnixMillis());
                }
                Status st = _submit_compaction_task(tablet, compaction_type, false);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to submit compaction task for tablet: "
                                 << tablet->tablet_id() << ", err: " << st;
                }
            }
            interval = config::generate_compaction_tasks_interval_ms;
        } else {
            interval = 5000; // 5s to check disable_auto_compaction
        }

        // wait some seconds for ut test
        {
            std ::vector<std ::any> args {};
            args.emplace_back(1);
            doris ::SyncPoint ::get_instance()->process(
                    "StorageEngine::_compaction_tasks_producer_callback", std ::move(args));
        }
        int64_t end_time = UnixMillis();
        DorisMetrics::instance()->compaction_producer_callback_a_round_time->set_value(end_time -
                                                                                       cur_time);
    } while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(interval)));
}

void StorageEngine::_update_replica_infos_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "start to update replica infos!";

    int64_t interval = config::update_replica_infos_interval_seconds;
    do {
        auto all_tablets = _tablet_manager->get_all_tablet([](Tablet* t) {
            return t->is_used() && t->tablet_state() == TABLET_RUNNING &&
                   !t->tablet_meta()->tablet_schema()->disable_auto_compaction() &&
                   t->tablet_meta()->tablet_schema()->enable_single_replica_compaction();
        });
        ClusterInfo* cluster_info = ExecEnv::GetInstance()->cluster_info();
        if (cluster_info == nullptr) {
            LOG(WARNING) << "Have not get FE Master heartbeat yet";
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }
        TNetworkAddress master_addr = cluster_info->master_fe_addr;
        if (master_addr.hostname == "" || master_addr.port == 0) {
            LOG(WARNING) << "Have not get FE Master heartbeat yet";
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }

        int start = 0;
        int tablet_size = cast_set<int>(all_tablets.size());
        // The while loop may take a long time, we should skip it when stop
        while (start < tablet_size && _stop_background_threads_latch.count() > 0) {
            int batch_size = std::min(100, tablet_size - start);
            int end = start + batch_size;
            TGetTabletReplicaInfosRequest request;
            TGetTabletReplicaInfosResult result;
            for (int i = start; i < end; i++) {
                request.tablet_ids.emplace_back(all_tablets[i]->tablet_id());
            }
            Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                    master_addr.hostname, master_addr.port,
                    [&request, &result](FrontendServiceConnection& client) {
                        client->getTabletReplicaInfos(result, request);
                    });

            if (!rpc_st.ok()) {
                LOG(WARNING) << "Failed to get tablet replica infos, encounter rpc failure, "
                                "tablet start: "
                             << start << " end: " << end;
                continue;
            }

            std::unique_lock<std::mutex> lock(_peer_replica_infos_mutex);
            for (const auto& it : result.tablet_replica_infos) {
                auto tablet_id = it.first;
                auto tablet = _tablet_manager->get_tablet(tablet_id);
                if (tablet == nullptr) {
                    VLOG_CRITICAL << "tablet ptr is nullptr";
                    continue;
                }

                VLOG_NOTICE << tablet_id << " tablet has " << it.second.size() << " replicas";
                uint64_t min_modulo = MOD_PRIME;
                TReplicaInfo peer_replica;
                for (const auto& replica : it.second) {
                    int64_t peer_replica_id = replica.replica_id;
                    uint64_t modulo = HashUtil::hash64(&peer_replica_id, sizeof(peer_replica_id),
                                                       DEFAULT_SEED) %
                                      MOD_PRIME;
                    if (modulo < min_modulo) {
                        peer_replica = replica;
                        min_modulo = modulo;
                    }
                }
                VLOG_NOTICE << "tablet " << tablet_id << ", peer replica host is "
                            << peer_replica.host;
                _peer_replica_infos[tablet_id] = peer_replica;
            }
            _token = result.token;
            VLOG_NOTICE << "get tablet replica infos from fe, size is " << end - start
                        << " token = " << result.token;
            start = end;
        }
        interval = config::update_replica_infos_interval_seconds;
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

Status StorageEngine::_submit_single_replica_compaction_task(TabletSharedPtr tablet,
                                                             CompactionType compaction_type) {
    // For single replica compaction, the local version to be merged is determined based on the version fetched from the peer replica.
    // Therefore, it is currently not possible to determine whether it should be a base compaction or cumulative compaction.
    // As a result, the tablet needs to be pushed to both the _tablet_submitted_cumu_compaction and the _tablet_submitted_base_compaction simultaneously.
    bool already_exist =
            _compaction_submit_registry.insert(tablet, CompactionType::CUMULATIVE_COMPACTION);
    if (already_exist) {
        return Status::AlreadyExist<false>(
                "compaction task has already been submitted, tablet_id={}", tablet->tablet_id());
    }

    already_exist = _compaction_submit_registry.insert(tablet, CompactionType::BASE_COMPACTION);
    if (already_exist) {
        _pop_tablet_from_submitted_compaction(tablet, CompactionType::CUMULATIVE_COMPACTION);
        return Status::AlreadyExist<false>(
                "compaction task has already been submitted, tablet_id={}", tablet->tablet_id());
    }

    auto compaction = std::make_shared<SingleReplicaCompaction>(*this, tablet, compaction_type);
    DorisMetrics::instance()->single_compaction_request_total->increment(1);
    auto st = compaction->prepare_compact();

    auto clean_single_replica_compaction = [tablet, this]() {
        _pop_tablet_from_submitted_compaction(tablet, CompactionType::CUMULATIVE_COMPACTION);
        _pop_tablet_from_submitted_compaction(tablet, CompactionType::BASE_COMPACTION);
    };

    if (!st.ok()) {
        clean_single_replica_compaction();
        if (!st.is<ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION>()) {
            LOG(WARNING) << "failed to prepare single replica compaction, tablet_id="
                         << tablet->tablet_id() << " : " << st;
            return st;
        }
        return Status::OK(); // No suitable version, regard as OK
    }

    auto submit_st = _single_replica_compaction_thread_pool->submit_func(
            [tablet, compaction = std::move(compaction),
             clean_single_replica_compaction]() mutable {
                tablet->execute_single_replica_compaction(*compaction);
                clean_single_replica_compaction();
            });
    if (!submit_st.ok()) {
        clean_single_replica_compaction();
        return Status::InternalError(
                "failed to submit single replica compaction task to thread pool, "
                "tablet_id={}",
                tablet->tablet_id());
    }
    return Status::OK();
}

void StorageEngine::get_tablet_rowset_versions(const PGetTabletVersionsRequest* request,
                                               PGetTabletVersionsResponse* response) {
    TabletSharedPtr tablet = _tablet_manager->get_tablet(request->tablet_id());
    if (tablet == nullptr) {
        response->mutable_status()->set_status_code(TStatusCode::CANCELLED);
        return;
    }
    std::vector<Version> local_versions = tablet->get_all_local_versions();
    for (const auto& local_version : local_versions) {
        auto version = response->add_versions();
        version->set_first(local_version.first);
        version->set_second(local_version.second);
    }
    response->mutable_status()->set_status_code(0);
}

bool need_generate_compaction_tasks(int task_cnt_per_disk, int thread_per_disk,
                                    CompactionType compaction_type, bool all_base) {
    // We need to reserve at least one Slot for cumulative compaction.
    // So when there is only one Slot, we have to judge whether there is a cumulative compaction
    // in the current submitted tasks.
    // If so, the last Slot can be assigned to Base compaction,
    // otherwise, this Slot needs to be reserved for cumulative compaction.
    if (task_cnt_per_disk >= thread_per_disk) {
        // Return if no available slot
        return false;
    } else if (task_cnt_per_disk >= thread_per_disk - 1) {
        // Only one slot left, check if it can be assigned to base compaction task.
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            if (all_base) {
                return false;
            }
        }
    }
    return true;
}

int get_concurrent_per_disk(int max_score, int thread_per_disk) {
    if (!config::enable_compaction_priority_scheduling) {
        return thread_per_disk;
    }

    double load_average = 0;
    if (DorisMetrics::instance()->system_metrics() != nullptr) {
        load_average = DorisMetrics::instance()->system_metrics()->get_load_average_1_min();
    }
    int num_cores = doris::CpuInfo::num_cores();
    bool cpu_usage_high = load_average > num_cores * 0.8;

    auto process_memory_usage = doris::GlobalMemoryArbitrator::process_memory_usage();
    bool memory_usage_high = static_cast<double>(process_memory_usage) >
                             static_cast<double>(MemInfo::soft_mem_limit()) * 0.8;

    if (max_score <= config::low_priority_compaction_score_threshold &&
        (cpu_usage_high || memory_usage_high)) {
        return config::low_priority_compaction_task_num_per_disk;
    }

    return thread_per_disk;
}

int32_t disk_compaction_slot_num(const DataDir& data_dir) {
    return data_dir.is_ssd_disk() ? config::compaction_task_num_per_fast_disk
                                  : config::compaction_task_num_per_disk;
}

bool has_free_compaction_slot(CompactionSubmitRegistry* registry, DataDir* dir,
                              CompactionType compaction_type, uint32_t executing_cnt) {
    int32_t thread_per_disk = disk_compaction_slot_num(*dir);
    return need_generate_compaction_tasks(
            executing_cnt, thread_per_disk, compaction_type,
            !registry->has_compaction_task(dir, CompactionType::CUMULATIVE_COMPACTION));
}

std::vector<TabletSharedPtr> StorageEngine::_generate_compaction_tasks(
        CompactionType compaction_type, std::vector<DataDir*>& data_dirs, bool check_score) {
    _update_cumulative_compaction_policy();
    std::vector<TabletSharedPtr> tablets_compaction;
    uint32_t max_compaction_score = 0;

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data_dirs.begin(), data_dirs.end(), g);

    // Copy _tablet_submitted_xxx_compaction map so that we don't need to hold _tablet_submitted_compaction_mutex
    // when traversing the data dir
    auto compaction_registry_snapshot = _compaction_submit_registry.create_snapshot();
    for (auto* data_dir : data_dirs) {
        bool need_pick_tablet = true;
        uint32_t executing_task_num =
                compaction_registry_snapshot.count_executing_cumu_and_base(data_dir);
        need_pick_tablet = has_free_compaction_slot(&compaction_registry_snapshot, data_dir,
                                                    compaction_type, executing_task_num);
        if (!need_pick_tablet && !check_score) {
            continue;
        }

        // Even if need_pick_tablet is false, we still need to call find_best_tablet_to_compaction(),
        // So that we can update the max_compaction_score metric.
        if (!data_dir->reach_capacity_limit(0)) {
            uint32_t disk_max_score = 0;
            auto tablets = compaction_registry_snapshot.pick_topn_tablets_for_compaction(
                    _tablet_manager.get(), data_dir, compaction_type,
                    _cumulative_compaction_policies, &disk_max_score);
            int concurrent_num =
                    get_concurrent_per_disk(disk_max_score, disk_compaction_slot_num(*data_dir));
            need_pick_tablet = need_generate_compaction_tasks(
                    executing_task_num, concurrent_num, compaction_type,
                    !compaction_registry_snapshot.has_compaction_task(
                            data_dir, CompactionType::CUMULATIVE_COMPACTION));
            for (const auto& tablet : tablets) {
                if (tablet != nullptr) {
                    if (need_pick_tablet) {
                        tablets_compaction.emplace_back(tablet);
                    }
                    max_compaction_score = std::max(max_compaction_score, disk_max_score);
                }
            }
        }
    }

    if (max_compaction_score > 0) {
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            DorisMetrics::instance()->tablet_base_max_compaction_score->set_value(
                    max_compaction_score);
        } else {
            DorisMetrics::instance()->tablet_cumulative_max_compaction_score->set_value(
                    max_compaction_score);
        }
    }
    return tablets_compaction;
}

void StorageEngine::_update_cumulative_compaction_policy() {
    if (_cumulative_compaction_policies.empty()) {
        _cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
                CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                        CUMULATIVE_SIZE_BASED_POLICY);
        _cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
                CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                        CUMULATIVE_TIME_SERIES_POLICY);
    }
}

void StorageEngine::_pop_tablet_from_submitted_compaction(TabletSharedPtr tablet,
                                                          CompactionType compaction_type) {
    _compaction_submit_registry.remove(tablet, compaction_type, [this]() {
        std::unique_lock<std::mutex> lock(_compaction_producer_sleep_mutex);
        _wakeup_producer_flag = 1;
        _compaction_producer_sleep_cv.notify_one();
    });
}

Status StorageEngine::_submit_compaction_task(TabletSharedPtr tablet,
                                              CompactionType compaction_type, bool force) {
    if (tablet->tablet_meta()->tablet_schema()->enable_single_replica_compaction() &&
        should_fetch_from_peer(tablet->tablet_id())) {
        VLOG_CRITICAL << "start to submit single replica compaction task for tablet: "
                      << tablet->tablet_id();
        Status st = _submit_single_replica_compaction_task(tablet, compaction_type);
        if (!st.ok()) {
            LOG(WARNING) << "failed to submit single replica compaction task for tablet: "
                         << tablet->tablet_id() << ", err: " << st;
        }

        return Status::OK();
    }
    bool already_exist = _compaction_submit_registry.insert(tablet, compaction_type);
    if (already_exist) {
        return Status::AlreadyExist<false>(
                "compaction task has already been submitted, tablet_id={}, compaction_type={}.",
                tablet->tablet_id(), compaction_type);
    }
    tablet->compaction_stage = CompactionStage::PENDING;
    std::shared_ptr<CompactionMixin> compaction;
    int64_t permits = 0;
    Status st = Tablet::prepare_compaction_and_calculate_permits(compaction_type, tablet,
                                                                 compaction, permits);
    if (st.ok() && permits > 0) {
        if (!force) {
            _permit_limiter.request(permits);
        }
        std::unique_ptr<ThreadPool>& thread_pool =
                (compaction_type == CompactionType::CUMULATIVE_COMPACTION)
                        ? _cumu_compaction_thread_pool
                        : _base_compaction_thread_pool;
        VLOG_CRITICAL << "compaction thread pool. type: "
                      << (compaction_type == CompactionType::CUMULATIVE_COMPACTION ? "CUMU"
                                                                                   : "BASE")
                      << ", num_threads: " << thread_pool->num_threads()
                      << ", num_threads_pending_start: " << thread_pool->num_threads_pending_start()
                      << ", num_active_threads: " << thread_pool->num_active_threads()
                      << ", max_threads: " << thread_pool->max_threads()
                      << ", min_threads: " << thread_pool->min_threads()
                      << ", num_total_queued_tasks: " << thread_pool->get_queue_size();
        auto status = thread_pool->submit_func([tablet, compaction = std::move(compaction),
                                                compaction_type, permits, force, this]() {
            if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) [[likely]] {
                DorisMetrics::instance()->cumulative_compaction_task_running_total->increment(1);
                DorisMetrics::instance()->cumulative_compaction_task_pending_total->set_value(
                        _cumu_compaction_thread_pool->get_queue_size());
            } else if (compaction_type == CompactionType::BASE_COMPACTION) {
                DorisMetrics::instance()->base_compaction_task_running_total->increment(1);
                DorisMetrics::instance()->base_compaction_task_pending_total->set_value(
                        _base_compaction_thread_pool->get_queue_size());
            }
            bool is_large_task = true;
            Defer defer {[&]() {
                DBUG_EXECUTE_IF("StorageEngine._submit_compaction_task.sleep", { sleep(5); })
                if (!force) {
                    _permit_limiter.release(permits);
                }
                _pop_tablet_from_submitted_compaction(tablet, compaction_type);
                tablet->compaction_stage = CompactionStage::NOT_SCHEDULED;
                if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                    std::lock_guard<std::mutex> lock(_cumu_compaction_delay_mtx);
                    _cumu_compaction_thread_pool_used_threads--;
                    if (!is_large_task) {
                        _cumu_compaction_thread_pool_small_tasks_running--;
                    }
                    DorisMetrics::instance()->cumulative_compaction_task_running_total->increment(
                            -1);
                    DorisMetrics::instance()->cumulative_compaction_task_pending_total->set_value(
                            _cumu_compaction_thread_pool->get_queue_size());
                } else if (compaction_type == CompactionType::BASE_COMPACTION) {
                    DorisMetrics::instance()->base_compaction_task_running_total->increment(-1);
                    DorisMetrics::instance()->base_compaction_task_pending_total->set_value(
                            _base_compaction_thread_pool->get_queue_size());
                }
            }};
            do {
                if (compaction->compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
                    std::lock_guard<std::mutex> lock(_cumu_compaction_delay_mtx);
                    _cumu_compaction_thread_pool_used_threads++;
                    if (config::large_cumu_compaction_task_min_thread_num > 1 &&
                        _cumu_compaction_thread_pool->max_threads() >=
                                config::large_cumu_compaction_task_min_thread_num) {
                        // Determine if this is a large task based on configured thresholds
                        is_large_task =
                                (compaction->calc_input_rowsets_total_size() >
                                         config::large_cumu_compaction_task_bytes_threshold ||
                                 compaction->calc_input_rowsets_row_num() >
                                         config::large_cumu_compaction_task_row_num_threshold);

                        // Small task. No delay needed
                        if (!is_large_task) {
                            _cumu_compaction_thread_pool_small_tasks_running++;
                            break;
                        }
                        // Deal with large task
                        if (_should_delay_large_task()) {
                            LOG_WARNING(
                                    "failed to do CumulativeCompaction, cumu thread pool is "
                                    "intensive, delay large task.")
                                    .tag("tablet_id", tablet->tablet_id())
                                    .tag("input_rows", compaction->calc_input_rowsets_row_num())
                                    .tag("input_rowsets_total_size",
                                         compaction->calc_input_rowsets_total_size())
                                    .tag("config::large_cumu_compaction_task_bytes_threshold",
                                         config::large_cumu_compaction_task_bytes_threshold)
                                    .tag("config::large_cumu_compaction_task_row_num_threshold",
                                         config::large_cumu_compaction_task_row_num_threshold)
                                    .tag("remaining threads",
                                         _cumu_compaction_thread_pool_used_threads)
                                    .tag("small_tasks_running",
                                         _cumu_compaction_thread_pool_small_tasks_running);
                            // Delay this task and sleep 5s for this tablet
                            long now = duration_cast<std::chrono::milliseconds>(
                                               std::chrono::system_clock::now().time_since_epoch())
                                               .count();
                            tablet->set_last_cumu_compaction_failure_time(now);
                            return;
                        }
                    }
                }
            } while (false);
            if (!tablet->can_do_compaction(tablet->data_dir()->path_hash(), compaction_type)) {
                LOG(INFO) << "Tablet state has been changed, no need to begin this compaction "
                             "task, tablet_id="
                          << tablet->tablet_id() << ", tablet_state=" << tablet->tablet_state();
                return;
            }
            tablet->compaction_stage = CompactionStage::EXECUTING;
            TEST_SYNC_POINT_RETURN_WITH_VOID("olap_server::execute_compaction");
            tablet->execute_compaction(*compaction);
        });
        if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) [[likely]] {
            DorisMetrics::instance()->cumulative_compaction_task_pending_total->set_value(
                    _cumu_compaction_thread_pool->get_queue_size());
        } else if (compaction_type == CompactionType::BASE_COMPACTION) {
            DorisMetrics::instance()->base_compaction_task_pending_total->set_value(
                    _base_compaction_thread_pool->get_queue_size());
        }
        if (!st.ok()) {
            if (!force) {
                _permit_limiter.release(permits);
            }
            _pop_tablet_from_submitted_compaction(tablet, compaction_type);
            tablet->compaction_stage = CompactionStage::NOT_SCHEDULED;
            return Status::InternalError(
                    "failed to submit compaction task to thread pool, "
                    "tablet_id={}, compaction_type={}.",
                    tablet->tablet_id(), compaction_type);
        }
        return Status::OK();
    } else {
        _pop_tablet_from_submitted_compaction(tablet, compaction_type);
        tablet->compaction_stage = CompactionStage::NOT_SCHEDULED;
        if (!st.ok()) {
            return Status::InternalError(
                    "failed to prepare compaction task and calculate permits, "
                    "tablet_id={}, compaction_type={}, "
                    "permit={}, current_permit={}, status={}",
                    tablet->tablet_id(), compaction_type, permits, _permit_limiter.usage(),
                    st.to_string());
        }
        return st;
    }
}

Status StorageEngine::submit_compaction_task(TabletSharedPtr tablet, CompactionType compaction_type,
                                             bool force, bool eager) {
    if (!eager) {
        DCHECK(compaction_type == CompactionType::BASE_COMPACTION ||
               compaction_type == CompactionType::CUMULATIVE_COMPACTION);
        auto compaction_registry_snapshot = _compaction_submit_registry.create_snapshot();
        auto stores = get_stores();

        bool is_busy = std::none_of(
                stores.begin(), stores.end(),
                [&compaction_registry_snapshot, compaction_type](auto* data_dir) {
                    return has_free_compaction_slot(
                            &compaction_registry_snapshot, data_dir, compaction_type,
                            compaction_registry_snapshot.count_executing_cumu_and_base(data_dir));
                });
        if (is_busy) {
            LOG_EVERY_N(WARNING, 100)
                    << "Too busy to submit a compaction task, tablet=" << tablet->get_table_id();
            return Status::OK();
        }
    }
    _update_cumulative_compaction_policy();
    // alter table tableName set ("compaction_policy"="time_series")
    // if atler table's compaction  policy, we need to modify tablet compaction policy shared ptr
    if (tablet->get_cumulative_compaction_policy() == nullptr ||
        tablet->get_cumulative_compaction_policy()->name() !=
                tablet->tablet_meta()->compaction_policy()) {
        tablet->set_cumulative_compaction_policy(
                _cumulative_compaction_policies.at(tablet->tablet_meta()->compaction_policy()));
    }
    tablet->set_skip_compaction(false);
    return _submit_compaction_task(tablet, compaction_type, force);
}

Status StorageEngine::_handle_seg_compaction(std::shared_ptr<SegcompactionWorker> worker,
                                             SegCompactionCandidatesSharedPtr segments,
                                             uint64_t submission_time) {
    // note: be aware that worker->_writer maybe released when the task is cancelled
    uint64_t exec_queue_time = GetCurrentTimeMicros() - submission_time;
    LOG(INFO) << "segcompaction thread pool queue time(ms): " << exec_queue_time / 1000;
    worker->compact_segments(segments);
    // return OK here. error will be reported via BetaRowsetWriter::_segcompaction_status
    return Status::OK();
}

Status StorageEngine::submit_seg_compaction_task(std::shared_ptr<SegcompactionWorker> worker,
                                                 SegCompactionCandidatesSharedPtr segments) {
    uint64_t submission_time = GetCurrentTimeMicros();
    return _seg_compaction_thread_pool->submit_func([this, worker, segments, submission_time] {
        static_cast<void>(_handle_seg_compaction(worker, segments, submission_time));
    });
}

Status StorageEngine::process_index_change_task(const TAlterInvertedIndexReq& request) {
    auto tablet_id = request.tablet_id;
    TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id);
    DBUG_EXECUTE_IF("StorageEngine::process_index_change_task_tablet_nullptr",
                    { tablet = nullptr; })
    if (tablet == nullptr) {
        LOG(WARNING) << "tablet: " << tablet_id << " not exist";
        return Status::InternalError("tablet not exist, tablet_id={}.", tablet_id);
    }

    IndexBuilderSharedPtr index_builder = std::make_shared<IndexBuilder>(
            *this, tablet, request.columns, request.alter_inverted_indexes, request.is_drop_op);
    RETURN_IF_ERROR(_handle_index_change(index_builder));
    return Status::OK();
}

Status StorageEngine::_handle_index_change(IndexBuilderSharedPtr index_builder) {
    RETURN_IF_ERROR(index_builder->init());
    RETURN_IF_ERROR(index_builder->do_build_inverted_index());
    return Status::OK();
}

void StorageEngine::_cooldown_tasks_producer_callback() {
    int64_t interval = config::generate_cooldown_task_interval_sec;
    // the cooldown replica may be slow to upload it's meta file, so we should wait
    // until it has done uploaded
    int64_t skip_failed_interval = interval * 10;
    do {
        // these tables are ordered by priority desc
        std::vector<TabletSharedPtr> tablets;
        std::vector<RowsetSharedPtr> rowsets;
        // TODO(luwei) : a more efficient way to get cooldown tablets
        auto cur_time = time(nullptr);
        // we should skip all the tablets which are not running and those pending to do cooldown
        // also tablets once failed to do follow cooldown
        auto skip_tablet = [this, skip_failed_interval,
                            cur_time](const TabletSharedPtr& tablet) -> bool {
            bool is_skip =
                    cur_time - tablet->last_failed_follow_cooldown_time() < skip_failed_interval ||
                    TABLET_RUNNING != tablet->tablet_state();
            if (is_skip) {
                return is_skip;
            }
            std::lock_guard<std::mutex> lock(_running_cooldown_mutex);
            return _running_cooldown_tablets.find(tablet->tablet_id()) !=
                   _running_cooldown_tablets.end();
        };
        _tablet_manager->get_cooldown_tablets(&tablets, &rowsets, std::move(skip_tablet));
        LOG(INFO) << "cooldown producer get tablet num: " << tablets.size();
        int max_priority = cast_set<int>(tablets.size());
        int index = 0;
        for (const auto& tablet : tablets) {
            {
                std::lock_guard<std::mutex> lock(_running_cooldown_mutex);
                _running_cooldown_tablets.insert(tablet->tablet_id());
            }
            PriorityThreadPool::Task task;
            RowsetSharedPtr rowset = std::move(rowsets[index++]);
            task.work_function = [tablet, rowset, task_size = tablets.size(), this]() {
                Status st = tablet->cooldown(rowset);
                {
                    std::lock_guard<std::mutex> lock(_running_cooldown_mutex);
                    _running_cooldown_tablets.erase(tablet->tablet_id());
                }
                if (!st.ok()) {
                    LOG(WARNING) << "failed to cooldown, tablet: " << tablet->tablet_id()
                                 << " err: " << st;
                } else {
                    LOG(INFO) << "succeed to cooldown, tablet: " << tablet->tablet_id()
                              << " cooldown progress ("
                              << task_size - _cooldown_thread_pool->get_queue_size() << "/"
                              << task_size << ")";
                }
            };
            task.priority = max_priority--;
            bool submited = _cooldown_thread_pool->offer(std::move(task));

            if (!submited) {
                LOG(INFO) << "failed to submit cooldown task";
            }
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval)));
}

void StorageEngine::_remove_unused_remote_files_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::remove_unused_remote_files_interval_sec))) {
        LOG(INFO) << "begin to remove unused remote files";
        do_remove_unused_remote_files();
    }
}

void StorageEngine::do_remove_unused_remote_files() {
    auto tablets = tablet_manager()->get_all_tablet([](Tablet* t) {
        return t->tablet_meta()->cooldown_meta_id().initialized() && t->is_used() &&
               t->tablet_state() == TABLET_RUNNING &&
               t->cooldown_conf_unlocked().cooldown_replica_id == t->replica_id();
    });
    TConfirmUnusedRemoteFilesRequest req;
    req.__isset.confirm_list = true;
    // tablet_id -> [storage_resource, unused_remote_files]
    using unused_remote_files_buffer_t =
            std::unordered_map<int64_t, std::pair<StorageResource, std::vector<io::FileInfo>>>;
    unused_remote_files_buffer_t buffer;
    int64_t num_files_in_buffer = 0;
    // assume a filename is 0.1KB, buffer size should not larger than 100MB
    constexpr int64_t max_files_in_buffer = 1000000;

    auto calc_unused_remote_files = [&req, &buffer, &num_files_in_buffer, this](Tablet* t) {
        auto storage_resource = get_resource_by_storage_policy_id(t->storage_policy_id());
        if (!storage_resource) {
            LOG(WARNING) << "encounter error when remove unused remote files, tablet_id="
                         << t->tablet_id() << " : " << storage_resource.error();
            return;
        }

        // TODO(plat1ko): Support path v1
        if (storage_resource->path_version > 0) {
            return;
        }

        std::vector<io::FileInfo> files;
        // FIXME(plat1ko): What if user reset resource in storage policy to another resource?
        //  Maybe we should also list files in previously uploaded resources.
        bool exists = true;
        auto st = storage_resource->fs->list(storage_resource->remote_tablet_path(t->tablet_id()),
                                             true, &files, &exists);
        if (!st.ok()) {
            LOG(WARNING) << "encounter error when remove unused remote files, tablet_id="
                         << t->tablet_id() << " : " << st;
            return;
        }
        if (!exists || files.empty()) {
            return;
        }
        // get all cooldowned rowsets
        RowsetIdUnorderedSet cooldowned_rowsets;
        UniqueId cooldown_meta_id;
        {
            std::shared_lock rlock(t->get_header_lock());
            for (auto&& rs_meta : t->tablet_meta()->all_rs_metas()) {
                if (!rs_meta->is_local()) {
                    cooldowned_rowsets.insert(rs_meta->rowset_id());
                }
            }
            if (cooldowned_rowsets.empty()) {
                return;
            }
            cooldown_meta_id = t->tablet_meta()->cooldown_meta_id();
        }
        auto [cooldown_term, cooldown_replica_id] = t->cooldown_conf();
        if (cooldown_replica_id != t->replica_id()) {
            return;
        }
        // {cooldown_replica_id}.{cooldown_term}.meta
        std::string remote_meta_path =
                cooldown_tablet_meta_filename(cooldown_replica_id, cooldown_term);
        // filter out the paths that should be reserved
        auto filter = [&, this](io::FileInfo& info) {
            std::string_view filename = info.file_name;
            if (filename.ends_with(".meta")) {
                return filename == remote_meta_path;
            }
            auto rowset_id = extract_rowset_id(filename);
            if (rowset_id.hi == 0) {
                return false;
            }
            return cooldowned_rowsets.contains(rowset_id) ||
                   pending_remote_rowsets().contains(rowset_id);
        };
        files.erase(std::remove_if(files.begin(), files.end(), std::move(filter)), files.end());
        if (files.empty()) {
            return;
        }
        files.shrink_to_fit();
        num_files_in_buffer += files.size();
        buffer.insert({t->tablet_id(), {*storage_resource, std::move(files)}});
        auto& info = req.confirm_list.emplace_back();
        info.__set_tablet_id(t->tablet_id());
        info.__set_cooldown_replica_id(cooldown_replica_id);
        info.__set_cooldown_meta_id(cooldown_meta_id.to_thrift());
    };

    auto confirm_and_remove_files = [&buffer, &req, &num_files_in_buffer]() {
        TConfirmUnusedRemoteFilesResult result;
        LOG(INFO) << "begin to confirm unused remote files. num_tablets=" << buffer.size()
                  << " num_files=" << num_files_in_buffer;
        auto st = MasterServerClient::instance()->confirm_unused_remote_files(req, &result);
        if (!st.ok()) {
            LOG(WARNING) << st;
            return;
        }
        for (auto id : result.confirmed_tablets) {
            if (auto it = buffer.find(id); LIKELY(it != buffer.end())) {
                auto& storage_resource = it->second.first;
                auto& files = it->second.second;
                std::vector<io::Path> paths;
                paths.reserve(files.size());
                // delete unused files
                LOG(INFO) << "delete unused files. root_path=" << storage_resource.fs->root_path()
                          << " tablet_id=" << id;
                io::Path dir = storage_resource.remote_tablet_path(id);
                for (auto& file : files) {
                    auto file_path = dir / file.file_name;
                    LOG(INFO) << "delete unused file: " << file_path.native();
                    paths.push_back(std::move(file_path));
                }
                st = storage_resource.fs->batch_delete(paths);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to delete unused files, tablet_id=" << id << " : "
                                 << st;
                }
                buffer.erase(it);
            }
        }
    };

    // batch confirm to reduce FE's overhead
    auto next_confirm_time = std::chrono::steady_clock::now() +
                             std::chrono::seconds(config::confirm_unused_remote_files_interval_sec);
    for (auto& t : tablets) {
        if (t.use_count() <= 1 // this means tablet has been dropped
            || t->cooldown_conf_unlocked().cooldown_replica_id != t->replica_id() ||
            t->tablet_state() != TABLET_RUNNING) {
            continue;
        }
        calc_unused_remote_files(t.get());
        if (num_files_in_buffer > 0 && (num_files_in_buffer > max_files_in_buffer ||
                                        std::chrono::steady_clock::now() > next_confirm_time)) {
            confirm_and_remove_files();
            buffer.clear();
            req.confirm_list.clear();
            num_files_in_buffer = 0;
            next_confirm_time =
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds(config::confirm_unused_remote_files_interval_sec);
        }
    }
    if (num_files_in_buffer > 0) {
        confirm_and_remove_files();
    }
}

void StorageEngine::_cold_data_compaction_producer_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::cold_data_compaction_interval_sec))) {
        if (config::disable_auto_compaction ||
            GlobalMemoryArbitrator::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
            continue;
        }

        std::unordered_set<int64_t> copied_tablet_submitted;
        {
            std::lock_guard lock(_cold_compaction_tablet_submitted_mtx);
            copied_tablet_submitted = _cold_compaction_tablet_submitted;
        }
        int64_t n = config::cold_data_compaction_thread_num - copied_tablet_submitted.size();
        if (n <= 0) {
            continue;
        }
        auto tablets = _tablet_manager->get_all_tablet([&copied_tablet_submitted](Tablet* t) {
            return t->tablet_meta()->cooldown_meta_id().initialized() && t->is_used() &&
                   t->tablet_state() == TABLET_RUNNING &&
                   !copied_tablet_submitted.contains(t->tablet_id()) &&
                   !t->tablet_meta()->tablet_schema()->disable_auto_compaction();
        });
        std::vector<std::pair<TabletSharedPtr, int64_t>> tablet_to_compact;
        tablet_to_compact.reserve(n + 1);
        std::vector<std::pair<TabletSharedPtr, int64_t>> tablet_to_follow;
        tablet_to_follow.reserve(n + 1);

        for (auto& t : tablets) {
            if (t->replica_id() == t->cooldown_conf_unlocked().cooldown_replica_id) {
                auto score = t->calc_cold_data_compaction_score();
                if (score < 4) {
                    continue;
                }
                tablet_to_compact.emplace_back(t, score);
                if (tablet_to_compact.size() > n) {
                    std::sort(tablet_to_compact.begin(), tablet_to_compact.end(),
                              [](auto& a, auto& b) { return a.second > b.second; });
                    tablet_to_compact.pop_back();
                }
                continue;
            }
            // else, need to follow
            {
                std::lock_guard lock(_running_cooldown_mutex);
                if (_running_cooldown_tablets.contains(t->table_id())) {
                    // already in cooldown queue
                    continue;
                }
            }
            // TODO(plat1ko): some avoidance strategy if failed to follow
            auto score = t->calc_cold_data_compaction_score();
            tablet_to_follow.emplace_back(t, score);

            if (tablet_to_follow.size() > n) {
                std::sort(tablet_to_follow.begin(), tablet_to_follow.end(),
                          [](auto& a, auto& b) { return a.second > b.second; });
                tablet_to_follow.pop_back();
            }
        }

        for (auto& [tablet, score] : tablet_to_compact) {
            LOG(INFO) << "submit cold data compaction. tablet_id=" << tablet->tablet_id()
                      << " score=" << score;
            static_cast<void>(_cold_data_compaction_thread_pool->submit_func(
                    [&, t = std::move(tablet), this]() {
                        auto compaction = std::make_shared<ColdDataCompaction>(*this, t);
                        {
                            std::lock_guard lock(_cold_compaction_tablet_submitted_mtx);
                            _cold_compaction_tablet_submitted.insert(t->tablet_id());
                        }
                        Defer defer {[&] {
                            std::lock_guard lock(_cold_compaction_tablet_submitted_mtx);
                            _cold_compaction_tablet_submitted.erase(t->tablet_id());
                        }};
                        std::unique_lock cold_compaction_lock(t->get_cold_compaction_lock(),
                                                              std::try_to_lock);
                        if (!cold_compaction_lock.owns_lock()) {
                            LOG(WARNING) << "try cold_compaction_lock failed, tablet_id="
                                         << t->tablet_id();
                            return;
                        }
                        _update_cumulative_compaction_policy();
                        if (t->get_cumulative_compaction_policy() == nullptr ||
                            t->get_cumulative_compaction_policy()->name() !=
                                    t->tablet_meta()->compaction_policy()) {
                            t->set_cumulative_compaction_policy(_cumulative_compaction_policies.at(
                                    t->tablet_meta()->compaction_policy()));
                        }

                        auto st = compaction->prepare_compact();
                        if (!st.ok()) {
                            LOG(WARNING) << "failed to prepare cold data compaction. tablet_id="
                                         << t->tablet_id() << " err=" << st;
                            return;
                        }

                        st = compaction->execute_compact();
                        if (!st.ok()) {
                            LOG(WARNING) << "failed to execute cold data compaction. tablet_id="
                                         << t->tablet_id() << " err=" << st;
                            return;
                        }
                    }));
        }

        for (auto& [tablet, score] : tablet_to_follow) {
            LOG(INFO) << "submit to follow cooldown meta. tablet_id=" << tablet->tablet_id()
                      << " score=" << score;
            static_cast<void>(_cold_data_compaction_thread_pool->submit_func([&,
                                                                              t = std::move(
                                                                                      tablet)]() {
                {
                    std::lock_guard lock(_cold_compaction_tablet_submitted_mtx);
                    _cold_compaction_tablet_submitted.insert(t->tablet_id());
                }
                auto st = t->cooldown();
                {
                    std::lock_guard lock(_cold_compaction_tablet_submitted_mtx);
                    _cold_compaction_tablet_submitted.erase(t->tablet_id());
                }
                if (!st.ok()) {
                    // The cooldown of the replica may be relatively slow
                    // resulting in a short period of time where following cannot be successful
                    LOG_EVERY_N(WARNING, 5)
                            << "failed to cooldown. tablet_id=" << t->tablet_id() << " err=" << st;
                }
            }));
        }
    }
}

void StorageEngine::add_async_publish_task(int64_t partition_id, int64_t tablet_id,
                                           int64_t publish_version, int64_t transaction_id,
                                           bool is_recovery) {
    if (!is_recovery) {
        bool exists = false;
        {
            std::shared_lock<std::shared_mutex> rlock(_async_publish_lock);
            if (auto tablet_iter = _async_publish_tasks.find(tablet_id);
                tablet_iter != _async_publish_tasks.end()) {
                if (auto iter = tablet_iter->second.find(publish_version);
                    iter != tablet_iter->second.end()) {
                    exists = true;
                }
            }
        }
        if (exists) {
            return;
        }
        TabletSharedPtr tablet = tablet_manager()->get_tablet(tablet_id);
        if (tablet == nullptr) {
            LOG(INFO) << "tablet may be dropped when add async publish task, tablet_id: "
                      << tablet_id;
            return;
        }
        PendingPublishInfoPB pending_publish_info_pb;
        pending_publish_info_pb.set_partition_id(partition_id);
        pending_publish_info_pb.set_transaction_id(transaction_id);
        static_cast<void>(TabletMetaManager::save_pending_publish_info(
                tablet->data_dir(), tablet->tablet_id(), publish_version,
                pending_publish_info_pb.SerializeAsString()));
    }
    LOG(INFO) << "add pending publish task, tablet_id: " << tablet_id
              << " version: " << publish_version << " txn_id:" << transaction_id
              << " is_recovery: " << is_recovery;
    std::unique_lock<std::shared_mutex> wlock(_async_publish_lock);
    _async_publish_tasks[tablet_id][publish_version] = {transaction_id, partition_id};
}

int64_t StorageEngine::get_pending_publish_min_version(int64_t tablet_id) {
    std::shared_lock<std::shared_mutex> rlock(_async_publish_lock);
    auto iter = _async_publish_tasks.find(tablet_id);
    if (iter == _async_publish_tasks.end()) {
        return INT64_MAX;
    }
    if (iter->second.empty()) {
        return INT64_MAX;
    }
    return iter->second.begin()->first;
}

void StorageEngine::_process_async_publish() {
    // tablet, publish_version
    std::vector<std::pair<TabletSharedPtr, int64_t>> need_removed_tasks;
    {
        std::unique_lock<std::shared_mutex> wlock(_async_publish_lock);
        for (auto tablet_iter = _async_publish_tasks.begin();
             tablet_iter != _async_publish_tasks.end();) {
            if (tablet_iter->second.empty()) {
                tablet_iter = _async_publish_tasks.erase(tablet_iter);
                continue;
            }
            int64_t tablet_id = tablet_iter->first;
            TabletSharedPtr tablet = tablet_manager()->get_tablet(tablet_id);
            if (!tablet) {
                LOG(WARNING) << "tablet does not exist when async publush, tablet_id: "
                             << tablet_id;
                tablet_iter = _async_publish_tasks.erase(tablet_iter);
                continue;
            }

            auto task_iter = tablet_iter->second.begin();
            int64_t version = task_iter->first;
            int64_t transaction_id = task_iter->second.first;
            int64_t partition_id = task_iter->second.second;
            int64_t max_version = tablet->max_version().second;

            if (version <= max_version) {
                need_removed_tasks.emplace_back(tablet, version);
                tablet_iter->second.erase(task_iter);
                tablet_iter++;
                continue;
            }
            if (version != max_version + 1) {
                int32_t max_version_config = tablet->max_version_config();
                // Keep only the most recent versions
                while (tablet_iter->second.size() > max_version_config) {
                    need_removed_tasks.emplace_back(tablet, version);
                    task_iter = tablet_iter->second.erase(task_iter);
                    version = task_iter->first;
                }
                tablet_iter++;
                continue;
            }

            auto async_publish_task = std::make_shared<AsyncTabletPublishTask>(
                    *this, tablet, partition_id, transaction_id, version);
            static_cast<void>(_tablet_publish_txn_thread_pool->submit_func(
                    [=]() { async_publish_task->handle(); }));
            tablet_iter->second.erase(task_iter);
            need_removed_tasks.emplace_back(tablet, version);
            tablet_iter++;
        }
    }
    for (auto& [tablet, publish_version] : need_removed_tasks) {
        static_cast<void>(TabletMetaManager::remove_pending_publish_info(
                tablet->data_dir(), tablet->tablet_id(), publish_version));
    }
}

void StorageEngine::_async_publish_callback() {
    while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(30))) {
        _process_async_publish();
    }
}

void StorageEngine::_check_tablet_delete_bitmap_score_callback() {
    LOG(INFO) << "try to start check tablet delete bitmap score!";
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::check_tablet_delete_bitmap_interval_seconds))) {
        if (!config::enable_check_tablet_delete_bitmap_score) {
            return;
        }
        uint64_t max_delete_bitmap_score = 0;
        uint64_t max_base_rowset_delete_bitmap_score = 0;
        _tablet_manager->get_topn_tablet_delete_bitmap_score(&max_delete_bitmap_score,
                                                             &max_base_rowset_delete_bitmap_score);
        if (max_delete_bitmap_score > 0) {
            _tablet_max_delete_bitmap_score_metrics->set_value(max_delete_bitmap_score);
        }
        if (max_base_rowset_delete_bitmap_score > 0) {
            _tablet_max_base_rowset_delete_bitmap_score_metrics->set_value(
                    max_base_rowset_delete_bitmap_score);
        }
    }
}
#include "common/compile_check_end.h"
} // namespace doris
