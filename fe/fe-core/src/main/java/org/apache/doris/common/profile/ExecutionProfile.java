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

package org.apache.doris.common.profile;

import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.thrift.TDetailedReportParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * root is used to collect profile of a complete query plan(including query or load).
 * Need to call addToProfileAsChild() to add it to the root profile.
 * It has the following structure:
 *  Execution Profile:
 *      Fragment 0:
 *          Instance 0:
 *          ...
 *      Fragment 1:
 *          Instance 0:
 *          ...
 *      ...
 *      LoadChannels:  // only for load job
 */
public class ExecutionProfile {
    private static final Logger LOG = LogManager.getLogger(ExecutionProfile.class);

    // The root profile of this execution task
    private RuntimeProfile root;
    // Profiles for each fragment. And the InstanceProfile is the child of fragment profile.
    // Which will be added to fragment profile when calling Coordinator::sendFragment()
    private List<RuntimeProfile> fragmentProfiles;
    // Profile for load channels. Only for load job.
    private RuntimeProfile loadChannelProfile;
    private boolean isPipelineXProfile = false;

    // use to merge profile from multi be
    private List<Map<TNetworkAddress, List<RuntimeProfile>>> multiBeProfile = null;
    // Profile using fragment id like 0,1,2,3,4. But fragment id in plan maybe not the same.
    // Should use a mapping to map from plan's id to profile's id.
    private Map<PlanFragmentId, Integer> fragmentIDIdx;

    public ExecutionProfile(TUniqueId queryId, List<PlanFragment> fragments) {
        int fragmentNum = fragments.size();
        fragmentIDIdx = Maps.newHashMap();
        int idx = 0;
        for (PlanFragment planFragment : fragments) {
            fragmentIDIdx.put(planFragment.getFragmentId(), idx);
            ++idx;
        }
        root = new RuntimeProfile("Execution Profile " + DebugUtil.printId(queryId));
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        root.addChild(fragmentsProfile);
        fragmentProfiles = Lists.newArrayList();
        multiBeProfile = Lists.newArrayList();
        for (int i = 0; i < fragmentNum; i++) {
            fragmentProfiles.add(new RuntimeProfile("Fragment " + i));
            fragmentsProfile.addChild(fragmentProfiles.get(i));
            multiBeProfile.add(new ConcurrentHashMap<TNetworkAddress, List<RuntimeProfile>>());
        }
        loadChannelProfile = new RuntimeProfile("LoadChannels");
        root.addChild(loadChannelProfile);
    }

    public void addMultiBeProfileByPipelineX(int profileFragmentId, TNetworkAddress address,
            List<RuntimeProfile> taskProfile) {
        multiBeProfile.get(profileFragmentId).put(address, taskProfile);
    }

    private List<List<RuntimeProfile>> getMultiBeProfile(int profileFragmentId) {
        Map<TNetworkAddress, List<RuntimeProfile>> multiPipeline = multiBeProfile.get(profileFragmentId);
        List<List<RuntimeProfile>> allPipelines = Lists.newArrayList();
        int pipelineSize = 0;
        for (List<RuntimeProfile> profiles : multiPipeline.values()) {
            pipelineSize = profiles.size();
            break;
        }
        for (int pipelineIdx = 0; pipelineIdx < pipelineSize; pipelineIdx++) {
            List<RuntimeProfile> allPipelineTask = new ArrayList<RuntimeProfile>();
            for (List<RuntimeProfile> pipelines : multiPipeline.values()) {
                RuntimeProfile pipeline = pipelines.get(pipelineIdx);
                for (Pair<RuntimeProfile, Boolean> runtimeProfile : pipeline.getChildList()) {
                    allPipelineTask.add(runtimeProfile.first);
                }
            }
            allPipelines.add(allPipelineTask);
        }
        return allPipelines;
    }

    private RuntimeProfile getPipelineXAggregatedProfile(Map<Integer, String> planNodeMap) {
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        for (int i = 0; i < fragmentProfiles.size(); ++i) {
            RuntimeProfile newFragmentProfile = new RuntimeProfile("Fragment " + i);
            fragmentsProfile.addChild(newFragmentProfile);
            List<List<RuntimeProfile>> allPipelines = getMultiBeProfile(i);
            int pipelineIdx = 0;
            for (List<RuntimeProfile> allPipelineTask : allPipelines) {
                RuntimeProfile mergedpipelineProfile = new RuntimeProfile(
                        "Pipeline : " + pipelineIdx + "(instance_num="
                                + allPipelineTask.size() + ")",
                        allPipelineTask.get(0).nodeId());
                RuntimeProfile.mergeProfiles(allPipelineTask, mergedpipelineProfile, planNodeMap);
                newFragmentProfile.addChild(mergedpipelineProfile);
                pipelineIdx++;
            }
        }
        return fragmentsProfile;
    }

    private RuntimeProfile getNonPipelineXAggregatedProfile(Map<Integer, String> planNodeMap) {
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        for (int i = 0; i < fragmentProfiles.size(); ++i) {
            RuntimeProfile oldFragmentProfile = fragmentProfiles.get(i);
            RuntimeProfile newFragmentProfile = new RuntimeProfile("Fragment " + i);
            fragmentsProfile.addChild(newFragmentProfile);
            List<RuntimeProfile> allInstanceProfiles = new ArrayList<RuntimeProfile>();
            for (Pair<RuntimeProfile, Boolean> runtimeProfile : oldFragmentProfile.getChildList()) {
                allInstanceProfiles.add(runtimeProfile.first);
            }
            RuntimeProfile mergedInstanceProfile = new RuntimeProfile("Instance" + "(instance_num="
                    + allInstanceProfiles.size() + ")", allInstanceProfiles.get(0).nodeId());
            newFragmentProfile.addChild(mergedInstanceProfile);
            RuntimeProfile.mergeProfiles(allInstanceProfiles, mergedInstanceProfile, planNodeMap);
        }
        return fragmentsProfile;
    }

    public RuntimeProfile getAggregatedFragmentsProfile(Map<Integer, String> planNodeMap) {
        if (isPipelineXProfile) {
            /*
             * Fragment 0
             * ---Pipeline 0
             * ------pipelineTask 0
             * ------pipelineTask 0
             * ------pipelineTask 0
             * ---Pipeline 1
             * ------pipelineTask 1
             * ---Pipeline 2
             * ------pipelineTask 2
             * ------pipelineTask 2
             * Fragment 1
             * ---Pipeline 0
             * ------......
             * ---Pipeline 1
             * ------......
             * ---Pipeline 2
             * ------......
             * ......
             */
            return getPipelineXAggregatedProfile(planNodeMap);
        } else {
            /*
             * Fragment 0
             * ---Instance 0
             * ------pipelineTask 0
             * ------pipelineTask 1
             * ------pipelineTask 2
             * ---Instance 1
             * ------pipelineTask 0
             * ------pipelineTask 1
             * ------pipelineTask 2
             * ---Instance 2
             * ------pipelineTask 0
             * ------pipelineTask 1
             * ------pipelineTask 2
             * Fragment 1
             * ---Instance 0
             * ---Instance 1
             * ---Instance 2
             * ......
             */
            return getNonPipelineXAggregatedProfile(planNodeMap);
        }
    }

    public RuntimeProfile getRoot() {
        return root;
    }

    public void setTotalTime(long totalTimeMs) {
        root.getCounterTotalTime().setValue(TUnit.TIME_MS, totalTimeMs);
    }

    public void finish() {
        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
            fragmentProfile.sortChildren();
        }
    }

    public void updateProfile(TReportExecStatusParams params, TNetworkAddress address) {
        if (isPipelineXProfile) {
            int pipelineIdx = 0;
            List<RuntimeProfile> taskProfile = Lists.newArrayList();
            for (TDetailedReportParams param : params.detailed_report) {
                String name = "Pipeline :" + pipelineIdx + " "
                        + " (host=" + address + ")";
                RuntimeProfile profile = new RuntimeProfile(name);
                taskProfile.add(profile);
                if (param.isSetProfile()) {
                    profile.update(param.profile);
                }
                if (params.done) {
                    profile.setIsDone(true);
                }
                pipelineIdx++;
            }
            // TODO ygl: is this right? there maybe multi Backends, what does
            // update load profile do???
            if (params.isSetLoadChannelProfile()) {
                loadChannelProfile.update(params.loadChannelProfile);
            }
            addMultiBeProfileByPipelineX(params.fragment_id, address,
                    taskProfile);
        }
    }

    public void addInstanceProfile(int fragmentId, RuntimeProfile instanceProfile) {
        Preconditions.checkArgument(fragmentId < fragmentProfiles.size(),
                fragmentId + " vs. " + fragmentProfiles.size());
        fragmentProfiles.get(fragmentId).addChild(instanceProfile);
    }
}
