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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.ClusterPB;
import org.apache.doris.cloud.proto.Cloud.ClusterPB.Type;
import org.apache.doris.cloud.proto.Cloud.ClusterStatus;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CloudClusterChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudClusterChecker.class);

    private CloudSystemInfoService cloudSystemInfoService;

    private final Object checkLock = new Object();

    boolean isUpdateCloudUniqueId = false;

    public CloudClusterChecker(CloudSystemInfoService cloudSystemInfoService) {
        super("cloud cluster check", Config.cloud_cluster_check_interval_second * 1000L);
        this.cloudSystemInfoService = cloudSystemInfoService;
    }

    /**
     * Diff 2 collections of current and the dest.
     * @param toAdd output param = (expectedState - currentState)
     * @param toDel output param = (currentState - expectedState)
     * @param supplierCurrentMapFunc get the current be or fe objects information map from memory, a lambda function
     * @param supplierNodeMapFunc get be or fe information map from meta_service return pb, a lambda function
     */
    private <T> void diffNodes(List<T> toAdd, List<T> toDel, Supplier<Map<String, T>> supplierCurrentMapFunc,
                               Supplier<Map<String, T>> supplierNodeMapFunc) {
        if (toAdd == null || toDel == null) {
            return;
        }

        // TODO(gavin): Consider VPC
        // vpc:ip:port -> Nodes
        Map<String, T> currentMap = supplierCurrentMapFunc.get();
        Map<String, T> nodeMap = supplierNodeMapFunc.get();

        if (LOG.isDebugEnabled()) {
            LOG.debug("current Nodes={} expected Nodes={}", currentMap.keySet(), nodeMap.keySet());
        }

        toDel.addAll(currentMap.keySet().stream().filter(i -> !nodeMap.containsKey(i))
                .map(currentMap::get).collect(Collectors.toList()));

        toAdd.addAll(nodeMap.keySet().stream().filter(i -> !currentMap.containsKey(i))
                .map(nodeMap::get).collect(Collectors.toList()));
    }

    private void checkToAddCluster(Map<String, ClusterPB> remoteClusterIdToPB, Set<String> localClusterIds) {
        List<String> toAddClusterIds = remoteClusterIdToPB.keySet().stream()
                .filter(i -> !localClusterIds.contains(i)).collect(Collectors.toList());
        toAddClusterIds.forEach(
                addId -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("begin to add clusterId: {}", addId);
                }
                // Attach tag to BEs
                String clusterName = remoteClusterIdToPB.get(addId).getClusterName();
                String clusterId = remoteClusterIdToPB.get(addId).getClusterId();
                String clusterPublicEndpoint = remoteClusterIdToPB.get(addId).getPublicEndpoint();
                String clusterPrivateEndpoint = remoteClusterIdToPB.get(addId).getPrivateEndpoint();
                // For old versions that do no have status field set
                ClusterStatus clusterStatus = remoteClusterIdToPB.get(addId).hasClusterStatus()
                        ? remoteClusterIdToPB.get(addId).getClusterStatus() : ClusterStatus.NORMAL;
                List<Backend> toAdd = new ArrayList<>();
                for (Cloud.NodeInfoPB node : remoteClusterIdToPB.get(addId).getNodesList()) {
                    String addr = Config.enable_fqdn_mode ? node.getHost() : node.getIp();
                    if (Strings.isNullOrEmpty(addr)) {
                        LOG.warn("cant get valid add from ms {}", node);
                        continue;
                    }
                    Backend b = new Backend(Env.getCurrentEnv().getNextId(), addr, node.getHeartbeatPort());
                    Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
                    newTagMap.put(Tag.CLOUD_CLUSTER_STATUS, String.valueOf(clusterStatus));
                    newTagMap.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
                    newTagMap.put(Tag.CLOUD_CLUSTER_ID, clusterId);
                    String nodePublicEndpoint = node.hasPublicEndpoint() && !node.getPublicEndpoint().isEmpty()
                            ? node.getPublicEndpoint()
                            : clusterPublicEndpoint;
                    String nodePrivateEndpoint = node.hasPrivateEndpoint() && !node.getPrivateEndpoint().isEmpty()
                            ? node.getPrivateEndpoint()
                            : clusterPrivateEndpoint;
                    newTagMap.put(Tag.PUBLIC_ENDPOINT, nodePublicEndpoint);
                    newTagMap.put(Tag.PRIVATE_ENDPOINT, nodePrivateEndpoint);
                    newTagMap.put(Tag.CLOUD_UNIQUE_ID, node.getCloudUniqueId());
                    b.setTagMap(newTagMap);
                    toAdd.add(b);
                }
                cloudSystemInfoService.updateCloudBackends(toAdd, new ArrayList<>());
            }
        );
    }

    private void checkToDelCluster(Map<String, ClusterPB> remoteClusterIdToPB, Set<String> localClusterIds,
                                   Map<String, List<Backend>> clusterIdToBackend) {
        List<String> toDelClusterIds = localClusterIds.stream()
                .filter(i -> !remoteClusterIdToPB.containsKey(i)).collect(Collectors.toList());
        // drop be cluster
        Map<String, List<Backend>> finalClusterIdToBackend = clusterIdToBackend;
        toDelClusterIds.forEach(
                delId -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("begin to drop clusterId: {}", delId);
                }
                List<Backend> toDel =
                        new ArrayList<>(finalClusterIdToBackend.getOrDefault(delId, new ArrayList<>()));
                cloudSystemInfoService.updateCloudBackends(new ArrayList<>(), toDel);
                // del clusterName
                String delClusterName = cloudSystemInfoService.getClusterNameByClusterId(delId);
                if (delClusterName.isEmpty()) {
                    LOG.warn("can't get delClusterName, clusterId: {}, plz check", delId);
                    return;
                }
                // del clusterID
                cloudSystemInfoService.dropCluster(delId, delClusterName);
            }
        );
    }

    private void updateStatus(List<Backend> currentBes, List<Cloud.NodeInfoPB> expectedBes, ClusterPB remoteClusterPb) {
        Map<String, Backend> currentMap = new HashMap<>();
        for (Backend be : currentBes) {
            String endpoint = be.getHost() + ":" + be.getHeartbeatPort();
            currentMap.put(endpoint, be);
        }

        for (Cloud.NodeInfoPB node : expectedBes) {
            String addr = Config.enable_fqdn_mode ? node.getHost() : node.getIp();
            if (Strings.isNullOrEmpty(addr)) {
                LOG.warn("cant get valid add from ms {}", node);
                continue;
            }
            String endpoint = addr + ":" + node.getHeartbeatPort();
            Cloud.NodeStatusPB status = node.getStatus();
            Backend be = currentMap.get(endpoint);
            if (be == null) {
                LOG.warn("cant get valid be {} from fe mem, ignore it checker will add this be at next", endpoint);
                continue;
            }

            if (status == Cloud.NodeStatusPB.NODE_STATUS_DECOMMISSIONING) {
                if (!be.isDecommissioned()) {
                    LOG.info("decommissioned backend: {} status: {}", be, status);
                    try {
                        ((CloudEnv) Env.getCurrentEnv()).getCloudUpgradeMgr().registerWaterShedTxnId(be.getId());
                    } catch (UserException e) {
                        LOG.warn("failed to register water shed txn id, decommission be {}", be.getId(), e);
                    }
                    be.setDecommissioning(true);
                }
            }

            if (status == Cloud.NodeStatusPB.NODE_STATUS_DECOMMISSIONED) {
                // When the synchronization status of the node is "NODE_STATUS_DECOMMISSIONED",
                // it indicates that the conditions for decommissioning have
                // already been checked in CloudTabletRebalancer.java,
                // such as the tablets having been successfully migrated and no remnants of WAL on the backend (BE).
                if (!be.isDecommissioned()) {
                    LOG.warn("impossible status, somewhere has bug,  backend: {} status: {}", be, status);
                }
                be.setDecommissioned(true);
                // edit log
                Env.getCurrentEnv().getEditLog().logBackendStateChange(be);
            }
            updateIfComputeNodeEndpointChanged(remoteClusterPb, node, be);
        }
    }

    private void updateIfComputeNodeEndpointChanged(ClusterPB remoteClusterPb, Cloud.NodeInfoPB node, Backend be) {
        // check PublicEndpoint、PrivateEndpoint is changed?
        boolean netChanged = false;
        String remotePublicEndpoint = node.hasPublicEndpoint() && !node.getPublicEndpoint().isEmpty()
                ? node.getPublicEndpoint()
                : remoteClusterPb.getPublicEndpoint();
        String localPublicEndpoint = be.getTagMap().get(Tag.PUBLIC_ENDPOINT);
        if (!localPublicEndpoint.equals(remotePublicEndpoint)) {
            LOG.info("be {} has changed public_endpoint from {} to {}",
                    be, localPublicEndpoint, remotePublicEndpoint);
            be.getTagMap().put(Tag.PUBLIC_ENDPOINT, remotePublicEndpoint);
            netChanged = true;
        }

        String remotePrivateEndpoint = node.hasPrivateEndpoint() && !node.getPrivateEndpoint().isEmpty()
                ? node.getPrivateEndpoint()
                : remoteClusterPb.getPrivateEndpoint();
        String localPrivateEndpoint = be.getTagMap().get(Tag.PRIVATE_ENDPOINT);
        if (!localPrivateEndpoint.equals(remotePrivateEndpoint)) {
            LOG.info("be {} has changed private_endpoint from {} to {}",
                    be, localPrivateEndpoint, remotePrivateEndpoint);
            be.getTagMap().put(Tag.PRIVATE_ENDPOINT, remotePrivateEndpoint);
            netChanged = true;
        }
        if (netChanged) {
            // edit log
            Env.getCurrentEnv().getEditLog().logBackendStateChange(be);
        }
    }

    private void checkDiffNode(Map<String, ClusterPB> remoteClusterIdToPB,
                               Map<String, List<Backend>> clusterIdToBackend) {
        for (String cid : clusterIdToBackend.keySet()) {
            List<Backend> toAdd = new ArrayList<>();
            List<Backend> toDel = new ArrayList<>();
            ClusterPB cp = remoteClusterIdToPB.get(cid);
            if (cp == null) {
                LOG.warn("can't get cid {} info, and local cluster info {}, remote cluster info {}",
                        cid, clusterIdToBackend, remoteClusterIdToPB);
                continue;
            }
            String newClusterName = cp.getClusterName();
            List<Backend> currentBes = clusterIdToBackend.getOrDefault(cid, new ArrayList<>());
            String currentClusterName = currentBes.stream().map(Backend::getCloudClusterName).findFirst().orElse("");

            if (!newClusterName.equals(currentClusterName)) {
                // rename cluster's name
                LOG.info("cluster_name corresponding to cluster_id has been changed,"
                        + " cluster_id : {} , current_cluster_name : {}, new_cluster_name :{}",
                        cid, currentClusterName, newClusterName);
                // change all be's cluster_name
                currentBes.forEach(b -> b.setCloudClusterName(newClusterName));
                // update clusterNameToId
                cloudSystemInfoService.updateClusterNameToId(newClusterName, currentClusterName, cid);
                // update tags
                currentBes.forEach(b -> Env.getCurrentEnv().getEditLog().logModifyBackend(b));
            }

            String currentClusterStatus = cloudSystemInfoService.getCloudStatusById(cid);

            // For old versions that do no have status field set
            ClusterStatus clusterStatus = cp.hasClusterStatus() ? cp.getClusterStatus() : ClusterStatus.NORMAL;
            String newClusterStatus = String.valueOf(clusterStatus);
            if (LOG.isDebugEnabled()) {
                LOG.debug("current cluster status {} {}", currentClusterStatus, newClusterStatus);
            }
            boolean needChange = false;
            // ATTN: found bug, In the same cluster, the cluster status in the tags of BE nodes is inconsistent.
            // Using a set to collect the cluster statuses from the BE nodes.
            Set<String> clusterStatusInMem = new HashSet<>();
            for (Backend backend : currentBes) {
                String beClusterStatus = backend.getTagMap().get(Tag.CLOUD_CLUSTER_STATUS);
                clusterStatusInMem.add(beClusterStatus == null ? "NOT_SET" : beClusterStatus);
            }
            if (clusterStatusInMem.size() != 1) {
                LOG.warn("cluster {}, multi be nodes cluster status inconsistent, fix it {}", cid, clusterStatusInMem);
                needChange = true;
            }
            if (!currentClusterStatus.equals(newClusterStatus) || needChange) {
                // cluster's status changed
                LOG.info("cluster_status corresponding to cluster_id has been changed,"
                        + " cluster_id : {} , current_cluster_status : {}, new_cluster_status :{}",
                        cid, currentClusterStatus, newClusterStatus);
                // change all be's cluster_status
                currentBes.forEach(b -> b.setCloudClusterStatus(newClusterStatus));
                // update tags
                currentBes.forEach(b -> Env.getCurrentEnv().getEditLog().logModifyBackend(b));
            }

            List<String> currentBeEndpoints = currentBes.stream().map(backend ->
                    backend.getHost() + ":" + backend.getHeartbeatPort()).collect(Collectors.toList());
            List<Cloud.NodeInfoPB> expectedBes = remoteClusterIdToPB.get(cid).getNodesList();
            List<String> remoteBeEndpoints = expectedBes.stream()
                    .map(pb -> {
                        String addr = Config.enable_fqdn_mode ? pb.getHost() : pb.getIp();
                        if (Strings.isNullOrEmpty(addr)) {
                            LOG.warn("cant get valid add from ms {}", pb);
                            return "";
                        }
                        return addr + ":" + pb.getHeartbeatPort();
                    }).filter(e -> !Strings.isNullOrEmpty(e))
                    .collect(Collectors.toList());
            LOG.info("get cloud cluster, clusterId={} local nodes={} remote nodes={}", cid,
                    currentBeEndpoints, remoteBeEndpoints);

            updateStatus(currentBes, expectedBes, remoteClusterIdToPB.get(cid));

            diffNodes(toAdd, toDel, () -> {
                Map<String, Backend> currentMap = new HashMap<>();
                for (Backend be : currentBes) {
                    String endpoint = be.getHost() + ":" + be.getHeartbeatPort();
                    currentMap.put(endpoint, be);
                }
                return currentMap;
            }, () -> {
                Map<String, Backend> nodeMap = new HashMap<>();
                for (Cloud.NodeInfoPB node : expectedBes) {
                    String host = Config.enable_fqdn_mode ? node.getHost() : node.getIp();
                    if (Strings.isNullOrEmpty(host)) {
                        LOG.warn("cant get valid add from ms {}", node);
                        continue;
                    }
                    String publicEndpoint = node.hasPublicEndpoint() && !node.getPublicEndpoint().isEmpty()
                            ? node.getPublicEndpoint()
                            : remoteClusterIdToPB.get(cid).getPublicEndpoint();
                    String privateEndpoint = node.hasPrivateEndpoint() && !node.getPrivateEndpoint().isEmpty()
                            ? node.getPrivateEndpoint()
                            : remoteClusterIdToPB.get(cid).getPrivateEndpoint();
                    String endpoint = host + ":" + node.getHeartbeatPort();
                    Backend b = new Backend(Env.getCurrentEnv().getNextId(), host, node.getHeartbeatPort());
                    if (node.hasIsSmoothUpgrade()) {
                        b.setSmoothUpgradeDst(node.getIsSmoothUpgrade());
                    }

                    // Attach tag to BEs
                    Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
                    newTagMap.put(Tag.CLOUD_CLUSTER_NAME, remoteClusterIdToPB.get(cid).getClusterName());
                    newTagMap.put(Tag.CLOUD_CLUSTER_ID, remoteClusterIdToPB.get(cid).getClusterId());
                    newTagMap.put(Tag.PUBLIC_ENDPOINT, publicEndpoint);
                    newTagMap.put(Tag.PRIVATE_ENDPOINT, privateEndpoint);
                    newTagMap.put(Tag.CLOUD_UNIQUE_ID, node.getCloudUniqueId());
                    b.setTagMap(newTagMap);
                    nodeMap.put(endpoint, b);
                }
                return nodeMap;
            });

            if (LOG.isDebugEnabled()) {
                LOG.debug("cluster_id: {}, diffBackends nodes: {}, current: {}, toAdd: {}, toDel: {}",
                        cid, expectedBes, currentBes, toAdd, toDel);
            }
            if (toAdd.isEmpty() && toDel.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("runAfterCatalogReady nothing todo");
                }
                continue;
            }

            cloudSystemInfoService.updateCloudBackends(toAdd, toDel);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        synchronized (checkLock) {
            checkCloudBackends();
            updateCloudMetrics();
            checkCloudFes();
        }
    }

    private void checkFeNodesMapValid() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin checkFeNodesMapValid");
        }
        Map<String, List<Backend>> clusterIdToBackend = cloudSystemInfoService.getCloudClusterIdToBackend();
        Set<String> clusterIds = new HashSet<>();
        Set<String> clusterNames = new HashSet<>();
        clusterIdToBackend.forEach((clusterId, bes) -> {
            if (bes.isEmpty()) {
                LOG.warn("impossible, somewhere err, clusterId {}, clusterIdToBeMap {}", clusterId, clusterIdToBackend);
                clusterIdToBackend.remove(clusterId);
            }
            bes.forEach(be -> {
                clusterIds.add(be.getCloudClusterId());
                clusterNames.add(be.getCloudClusterName());
            });
        });

        Map<String, String> nameToId = cloudSystemInfoService.getCloudClusterNameToId();
        nameToId.forEach((clusterName, clusterId) -> {
            if (!clusterIdToBackend.containsKey(clusterId)) {
                LOG.warn("impossible, somewhere err, clusterId {}, clusterName {}, clusterNameToIdMap {}",
                        clusterId, clusterName, nameToId);
                nameToId.remove(clusterName);
            }
        });

        if (!clusterNames.containsAll(nameToId.keySet()) || !nameToId.keySet().containsAll(clusterNames)) {
            LOG.warn("impossible, somewhere err, clusterNames {}, nameToId {}", clusterNames, nameToId);
        }
        if (!clusterIds.containsAll(nameToId.values()) || !nameToId.values().containsAll(clusterIds)) {
            LOG.warn("impossible, somewhere err, clusterIds {}, nameToId {}", clusterIds, nameToId);
        }
        if (!clusterIds.containsAll(clusterIdToBackend.keySet())
                || !clusterIdToBackend.keySet().containsAll(clusterIds)) {
            LOG.warn("impossible, somewhere err, clusterIds {}, clusterIdToBackend {}",
                    clusterIds, clusterIdToBackend);
        }
    }

    private void checkCloudFes() {
        Cloud.GetClusterResponse response = cloudSystemInfoService.getCloudCluster(
                Config.cloud_sql_server_cluster_name, Config.cloud_sql_server_cluster_id, "");
        if (!response.hasStatus() || !response.getStatus().hasCode()
                || response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("failed to get cloud cluster due to incomplete response, "
                    + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            return;
        }
        // Note: get_cluster interface cluster(option -> repeated), so it has at least one cluster.
        if (response.getClusterCount() == 0) {
            LOG.warn("meta service error , return cluster zero, plz check it, "
                    + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            return;
        }

        ClusterPB cpb = response.getCluster(0);
        if (LOG.isDebugEnabled()) {
            LOG.debug("get cloud cluster, clusterId={} nodes={}",
                    Config.cloud_sql_server_cluster_id, cpb.getNodesList());
        }
        List<Frontend> currentFollowers = Env.getCurrentEnv().getFrontends(FrontendNodeType.FOLLOWER);
        List<Frontend> currentObservers = Env.getCurrentEnv().getFrontends(FrontendNodeType.OBSERVER);
        currentFollowers.addAll(currentObservers);
        List<Frontend> currentFes = new ArrayList<>(currentFollowers.stream().collect(Collectors.toMap(
                fe -> fe.getHost() + ":" + fe.getEditLogPort(),
                fe -> fe,
                (existing, replacement) -> existing
        )).values());
        List<Frontend> toAdd = new ArrayList<>();
        List<Frontend> toDel = new ArrayList<>();
        List<Cloud.NodeInfoPB> expectedFes = cpb.getNodesList();

        if (!isUpdateCloudUniqueId) {
            // Just run once and number of fes is small, so iterating is ok.
            // newly addde fe has cloudUniqueId.
            for (Frontend fe : currentFes) {
                for (Cloud.NodeInfoPB node : expectedFes) {
                    if (fe.getHost().equals(Config.enable_fqdn_mode ? node.getHost() : node.getIp())
                            && fe.getEditLogPort() == node.getEditLogPort()) {
                        fe.setCloudUniqueId(node.getCloudUniqueId());
                        LOG.info("update cloud unique id result {}", fe);
                        break;
                    }
                }
            }
            isUpdateCloudUniqueId = true;
        }

        diffNodes(toAdd, toDel, () -> {
            // memory
            Map<String, Frontend> currentMap = new HashMap<>();
            String selfNode = Env.getCurrentEnv().getSelfNode().getIdent();
            for (Frontend fe : currentFes) {
                String endpoint = fe.getHost() + "_" + fe.getEditLogPort();
                if (selfNode.equals(endpoint)) {
                    continue;
                }
                // add type to map key, for diff
                endpoint = endpoint + "_" + fe.getRole();
                currentMap.put(endpoint, fe);
            }
            LOG.info("fes in memory {}", currentMap);
            return currentMap;
        }, () -> {
            // meta service
            Map<String, Frontend> nodeMap = new HashMap<>();
            String selfNode = Env.getCurrentEnv().getSelfNode().getIdent();
            for (Cloud.NodeInfoPB node : expectedFes) {
                String host = Config.enable_fqdn_mode ? node.getHost() : node.getIp();
                if (Strings.isNullOrEmpty(host)) {
                    LOG.warn("cant get valid add from ms {}", node);
                    continue;
                }
                String endpoint = host + "_" + node.getEditLogPort();
                if (selfNode.equals(endpoint)) {
                    continue;
                }
                Cloud.NodeInfoPB.NodeType type = node.getNodeType();
                FrontendNodeType role = type == Cloud.NodeInfoPB.NodeType.FE_OBSERVER
                        ? FrontendNodeType.OBSERVER :  FrontendNodeType.FOLLOWER;
                Frontend fe = new Frontend(role,
                        CloudEnv.genFeNodeNameFromMeta(host, node.getEditLogPort(),
                        node.getCtime() * 1000), host, node.getEditLogPort());
                fe.setCloudUniqueId(node.getCloudUniqueId());
                // add type to map key, for diff
                endpoint = endpoint + "_" + fe.getRole();
                nodeMap.put(endpoint, fe);
            }
            LOG.info("fes in ms {}", nodeMap);

            return nodeMap;
        });
        LOG.info("diffFrontends nodes: {}, current: {}, toAdd: {}, toDel: {}, enable auto start: {}",
                expectedFes, currentFes, toAdd, toDel, Config.enable_auto_start_for_cloud_cluster);
        if (toAdd.isEmpty() && toDel.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("runAfterCatalogReady getObserverFes nothing todo");
            }
            return;
        }
        try {
            cloudSystemInfoService.updateFrontends(toAdd, toDel);
        } catch (DdlException e) {
            LOG.warn("update cloud frontends exception e: {}, msg: {}", e, e.getMessage());
        }
    }

    private void checkCloudBackends() {
        Map<String, List<Backend>> clusterIdToBackend = cloudSystemInfoService.getCloudClusterIdToBackend();
        //rpc to ms, to get mysql user can use cluster_id
        // NOTE: rpc args all empty, use cluster_unique_id to get a instance's all cluster info.
        Cloud.GetClusterResponse response = cloudSystemInfoService.getCloudCluster("", "", "");
        if (!response.hasStatus() || !response.getStatus().hasCode()
                || (response.getStatus().getCode() != Cloud.MetaServiceCode.OK
                && response.getStatus().getCode() != MetaServiceCode.CLUSTER_NOT_FOUND)) {
            LOG.warn("failed to get cloud cluster due to incomplete response, "
                    + "cloud_unique_id={}, response={}", Config.cloud_unique_id, response);
            return;
        }
        Set<String> localClusterIds = clusterIdToBackend.keySet();
        // clusterId -> clusterPB
        Map<String, ClusterPB> remoteClusterIdToPB = response.getClusterList().stream()
                .filter(c -> c.getType() != Type.SQL)
                .collect(Collectors.toMap(ClusterPB::getClusterId, clusterPB -> clusterPB));
        LOG.info("get cluster info  clusterIds: {}", remoteClusterIdToPB);

        try {
            // cluster_ids diff remote <clusterId, nodes> and local <clusterId, nodes>
            // remote - local > 0, add bes to local
            checkToAddCluster(remoteClusterIdToPB, localClusterIds);

            // local - remote > 0, drop bes from local
            checkToDelCluster(remoteClusterIdToPB, localClusterIds, clusterIdToBackend);

            clusterIdToBackend = cloudSystemInfoService.getCloudClusterIdToBackend();

            if (remoteClusterIdToPB.keySet().size() != clusterIdToBackend.keySet().size()) {
                LOG.warn("impossible cluster id size not match, check it local {}, remote {}",
                        clusterIdToBackend, remoteClusterIdToPB);
            }
            // clusterID local == remote, diff nodes
            checkDiffNode(remoteClusterIdToPB, clusterIdToBackend);

            // check mem map
            checkFeNodesMapValid();
        } catch (Exception e) {
            LOG.warn("diff cluster has exception, {}", e.getMessage(), e);

        }
        LOG.info("daemon cluster get cluster info succ, current cloudClusterIdToBackendMap: {} clusterNameToId {}",
                cloudSystemInfoService.getCloudClusterIdToBackend(), cloudSystemInfoService.getCloudClusterNameToId());
    }

    private void updateCloudMetrics() {
        // Metric
        Map<String, List<Backend>> clusterIdToBackend = cloudSystemInfoService.getCloudClusterIdToBackend();
        Map<String, String> clusterNameToId = cloudSystemInfoService.getCloudClusterNameToId();
        for (Map.Entry<String, String> entry : clusterNameToId.entrySet()) {
            int aliveNum = 0;
            List<Backend> bes = clusterIdToBackend.get(entry.getValue());
            if (bes == null || bes.isEmpty()) {
                LOG.info("cant get be nodes by cluster {}, bes {}", entry, bes);
                continue;
            }
            for (Backend backend : bes) {
                MetricRepo.updateClusterBackendAlive(entry.getKey(), entry.getValue(),
                        backend.getAddress(), backend.isAlive());
                aliveNum = backend.isAlive() ? aliveNum + 1 : aliveNum;
            }
            MetricRepo.updateClusterBackendAliveTotal(entry.getKey(), entry.getValue(), aliveNum);
        }
    }

    public void checkNow() {
        if (Env.getCurrentEnv().isMaster()) {
            synchronized (checkLock) {
                runAfterCatalogReady();
            }
        }
    }
}
