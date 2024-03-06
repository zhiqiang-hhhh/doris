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

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TReportExecStatusParams;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Profile is a class to record the execution time of a query. It has the
 * following structure: root profile: // summary of this profile, such as start
 * time, end time, query id, etc. [SummaryProfile] // each execution profile is
 * a complete execution of a query, a job may contain multiple queries.
 * [List<ExecutionProfile>]
 *
 * SummaryProfile: Summary: Execution Summary:
 *
 *
 * ExecutionProfile: Fragment 0: Fragment 1: ...
 * And also summary profile contains plan information, but execution profile is for
 * be execution time.
 * StmtExecutor(Profile) ---> Coordinator(ExecutionProfile)
 */
public class Profile {
    private static final Logger LOG = LogManager.getLogger(Profile.class);
    private static final int MergedProfileLevel = 1;
    private RuntimeProfile rootProfile;
    private SummaryProfile summaryProfile;
    private AggregatedProfile aggregatedProfile;
    private ExecutionProfile executionProfile;
    private boolean isFinished;
    private Map<Integer, String> planNodeMap;

    private int profileLevel = 3;

    public Profile(String name, boolean isEnable, int profileLevel, boolean isPipelineX) {
        this.rootProfile = new RuntimeProfile(name);
        this.summaryProfile = new SummaryProfile(rootProfile);
        // if disabled, just set isFinished to true, so that update() will do nothing
        this.isFinished = !isEnable;
        this.profileLevel = profileLevel;
        this.rootProfile.setIsPipelineX(isPipelineX);
    }

    public void setExecutionProfile(ExecutionProfile executionProfile) {
        if (executionProfile == null) {
            LOG.warn("try to set a null excecution profile, it is abnormal", new Exception());
            return;
        }
        this.executionProfile = executionProfile;
        this.rootProfile.addChild(this.executionProfile.getRoot());
        this.aggregatedProfile = new AggregatedProfile(rootProfile, executionProfile);
    }

    // Some load tasks use this API to update the exec time directly.
    public synchronized void setExecutionTime(long totalTimeMs) {
        this.executionProfile.setTotalTime(totalTimeMs);
    }

    // This API will also add the profile to ProfileManager, so that we could get the profile from ProfileManager
    public synchronized void updateSummary(long startTime, Map<String, String> summaryInfo, boolean isFinished,
            Planner planner) {
        try {
            if (this.isFinished) {
                return;
            }
            if (executionProfile == null) {
                // Sometimes execution profile is not set
                return;
            }
            summaryProfile.update(summaryInfo);
            this.executionProfile.setTotalTime(TimeUtils.getElapsedTimeMs(startTime));
            rootProfile.computeTimeInProfile();
            // Nerids native insert not set planner, so it is null
            if (planner != null) {
                this.planNodeMap = planner.getExplainStringMap();
            }
            ProfileManager.getInstance().pushProfile(this);
            this.isFinished = isFinished;
            if (this.isFinished) {
                this.executionProfile.finish();
            }
        } catch (Throwable t) {
            LOG.warn("update profile failed", t);
            throw t;
        }
    }

    public synchronized void updateExecution(TReportExecStatusParams params, TNetworkAddress address) {
        this.executionProfile.updateProfile(params, address);
    }

    public RuntimeProfile getRootProfile() {
        return this.rootProfile;
    }

    public SummaryProfile getSummaryProfile() {
        return summaryProfile;
    }

    public String getProfileByLevel() {
        StringBuilder builder = new StringBuilder();
        // add summary to builder
        summaryProfile.prettyPrint(builder);
        LOG.info(builder.toString());
        if (this.profileLevel == MergedProfileLevel) {
            try {
                builder.append("\n MergedProfile \n");
                aggregatedProfile.getAggregatedFragmentsProfile(planNodeMap).prettyPrint(builder, "     ");
            } catch (Throwable aggProfileException) {
                LOG.warn("build merged simple profile failed", aggProfileException);
                builder.append("build merged simple profile failed");
            }
        }
        try {
            builder.append("\n");
            executionProfile.getRoot().prettyPrint(builder, "");
        } catch (Throwable aggProfileException) {
            LOG.warn("build profile failed", aggProfileException);
            builder.append("build  profile failed");
        }
        return builder.toString();
    }

    public String getProfileBrief() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(rootProfile.toBrief());
    }
}
