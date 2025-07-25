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

package org.apache.doris.datasource;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.hive.source.HiveSplit;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSplitSource;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * FileQueryScanNode for querying the file access type of catalog, now only support
 * hive, hudi, iceberg and TVF.
 */
public abstract class FileQueryScanNode extends FileScanNode {
    private static final Logger LOG = LogManager.getLogger(FileQueryScanNode.class);

    protected Map<String, SlotDescriptor> destSlotDescByName;
    protected TFileScanRangeParams params;

    @Getter
    protected TableSample tableSample;

    protected String brokerName;

    protected TableSnapshot tableSnapshot;
    // Save the reference of session variable, so that we don't need to get it from connection context.
    // connection context is a thread local variable, it is not available is running in other thread.
    protected SessionVariable sessionVariable;

    protected TableScanParams scanParams;

    /**
     * External file scan node for Query hms table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public FileQueryScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
            StatisticalType statisticalType, boolean needCheckColumnPriv,
            SessionVariable sv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
        this.sessionVariable = sv;
    }

    /**
     * Init ExternalFileScanNode, ONLY used for Nereids. Should NOT use this function in anywhere else.
     */
    @Override
    public void init() throws UserException {
        super.init();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setInitScanNodeStartTime();
        }
        doInitialize();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setInitScanNodeFinishTime();
        }
    }

    // Init scan provider and schema related params.
    protected void doInitialize() throws UserException {
        Preconditions.checkNotNull(desc);
        if (desc.getTable() instanceof ExternalTable) {
            ExternalTable table = (ExternalTable) desc.getTable();
            if (table.isView()) {
                throw new AnalysisException(
                        String.format("Querying external view '%s.%s' is not supported", table.getDbName(),
                                table.getName()));
            }
        }
        initBackendPolicy();
        initSchemaParams();
    }

    // Init schema (Tuple/Slot) related params.
    protected void initSchemaParams() throws UserException {
        destSlotDescByName = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            destSlotDescByName.put(slot.getColumn().getName(), slot);
        }
        params = new TFileScanRangeParams();
        params.setDestTupleId(desc.getId().asInt());
        List<String> partitionKeys = getPathPartitionKeys();
        List<Column> columns = desc.getTable().getBaseSchema(false);
        params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));
            params.addToRequiredSlots(slotInfo);
        }
        setDefaultValueExprs(getTargetTable(), destSlotDescByName, null, params, false);
        setColumnPositionMapping();
        // For query, set src tuple id to -1.
        params.setSrcTupleId(-1);
    }

    private void updateRequiredSlots() throws UserException {
        params.unsetRequiredSlots();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!getPathPartitionKeys().contains(slot.getColumn().getName()));
            params.addToRequiredSlots(slotInfo);
        }
        // Update required slots and column_idxs in scanRangeLocations.
        setColumnPositionMapping();
    }

    public void setTableSample(TableSample tSample) {
        this.tableSample = tSample;
    }

    @Override
    public void finalizeForNereids() throws UserException {
        doFinalize();
    }

    // Create scan range locations and the statistics.
    protected void doFinalize() throws UserException {
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setFinalizeScanNodeStartTime();
        }
        convertPredicate();
        createScanRangeLocations();
        updateRequiredSlots();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setFinalizeScanNodeFinishTime();
        }
    }

    /**
     * Used as a predicate to convert conjuncts into corresponding data sources.
     * All predicate conversions from different data sources should override this method.
     * and this method must be called in finalize,
     * because in nereids planner, conjuncts are only generated in the finalize stage.
     */
    protected void convertPredicate() {
    }

    private void setColumnPositionMapping()
            throws UserException {
        TableIf tbl = getTargetTable();
        List<Integer> columnIdxs = Lists.newArrayList();
        // avoid null pointer, it maybe has no slots when two tables are joined
        if (params.getRequiredSlots() == null) {
            params.setColumnIdxs(columnIdxs);
            return;
        }
        for (TFileScanSlotInfo slot : params.getRequiredSlots()) {
            if (!slot.isIsFileSlot()) {
                continue;
            }
            SlotDescriptor slotDesc = desc.getSlot(slot.getSlotId());
            String colName = slotDesc.getColumn().getName();
            if (colName.startsWith(Column.GLOBAL_ROWID_COL)) {
                continue;
            }

            int idx = -1;
            List<Column> columns = getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getName().equals(colName)) {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) {
                throw new UserException("Column " + colName + " not found in table " + tbl.getName());
            }
            columnIdxs.add(idx);
        }
        params.setColumnIdxs(columnIdxs);
    }

    public TFileScanRangeParams getFileScanRangeParams() {
        return params;
    }

    // Set some parameters of scan to support different types of file data sources
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
    }

    // Serialize the table to be scanned to BE's jni reader
    protected Optional<String> getSerializedTable() {
        return Optional.empty();
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        long start = System.currentTimeMillis();
        StmtExecutor executor = ConnectContext.get().getExecutor();
        if (executor != null) {
            executor.getSummaryProfile().setGetSplitsStartTime();
        }
        TFileFormatType fileFormatType = getFileFormatType();
        if (fileFormatType == TFileFormatType.FORMAT_ORC) {
            genSlotToSchemaIdMapForOrc();
        }
        params.setFormatType(fileFormatType);
        boolean isCsvOrJson = Util.isCsvFormat(fileFormatType) || fileFormatType == TFileFormatType.FORMAT_JSON
                || fileFormatType == TFileFormatType.FORMAT_TEXT;
        boolean isWal = fileFormatType == TFileFormatType.FORMAT_WAL;
        if (isCsvOrJson || isWal) {
            params.setFileAttributes(getFileAttributes());
            if (isFileStreamType()) {
                params.setFileType(TFileType.FILE_STREAM);
                FunctionGenTable table = (FunctionGenTable) this.desc.getTable();
                ExternalFileTableValuedFunction tableValuedFunction = (ExternalFileTableValuedFunction) table.getTvf();
                params.setCompressType(tableValuedFunction.getTFileCompressType());

                TScanRangeLocations curLocations = newLocations();
                TFileRangeDesc rangeDesc = new TFileRangeDesc();
                rangeDesc.setLoadId(ConnectContext.get().queryId());
                rangeDesc.setSize(-1);
                rangeDesc.setFileSize(-1);
                curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                curLocations.getScanRange().getExtScanRange().getFileScanRange().setParams(params);

                TScanRangeLocation location = new TScanRangeLocation();
                long backendId = ConnectContext.get().getBackendId();
                Backend backend = Env.getCurrentSystemInfo().getBackendsByCurrentCluster().get(backendId);
                location.setBackendId(backendId);
                location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
                curLocations.addToLocations(location);
                scanRangeLocations.add(curLocations);
                scanBackendIds.add(backendId);
                return;
            }
        }

        Map<String, String> locationProperties = getLocationProperties();
        // for JNI, only need to set properties
        if (fileFormatType == TFileFormatType.FORMAT_JNI) {
            params.setProperties(locationProperties);
        }

        int numBackends = backendPolicy.numBackends();
        List<String> pathPartitionKeys = getPathPartitionKeys();
        if (isBatchMode()) {
            // File splits are generated lazily, and fetched by backends while scanning.
            // Only provide the unique ID of split source to backend.
            splitAssignment = new SplitAssignment(
                    backendPolicy, this, this::splitToScanRange, locationProperties, pathPartitionKeys);
            splitAssignment.init();
            if (executor != null) {
                executor.getSummaryProfile().setGetSplitsFinishTime();
            }
            if (splitAssignment.getSampleSplit() == null && !isFileStreamType()) {
                return;
            }
            FileSplit fileSplit = (FileSplit) splitAssignment.getSampleSplit();
            TFileType locationType = fileSplit.getLocationType();
            selectedSplitNum = numApproximateSplits();
            if (selectedSplitNum < 0) {
                throw new UserException("Approximate split number should not be negative");
            }
            totalFileSize = fileSplit.getLength() * selectedSplitNum;
            long maxWaitTime = sessionVariable.getFetchSplitsMaxWaitTime();
            // Not accurate, only used to estimate concurrency.
            // Here, we must take the max of 1, because
            // in the case of multiple BEs, `numApproximateSplits() / backendPolicy.numBackends()` may be 0,
            // and finally numSplitsPerBE is 0, resulting in no data being queried.
            int numSplitsPerBE = Math.max(selectedSplitNum / backendPolicy.numBackends(), 1);
            for (Backend backend : backendPolicy.getBackends()) {
                SplitSource splitSource = new SplitSource(backend, splitAssignment, maxWaitTime);
                splitSources.add(splitSource);
                Env.getCurrentEnv().getSplitSourceManager().registerSplitSource(splitSource);
                TScanRangeLocations curLocations = newLocations();
                TSplitSource tSource = new TSplitSource();
                tSource.setSplitSourceId(splitSource.getUniqueId());
                tSource.setNumSplits(numSplitsPerBE);
                curLocations.getScanRange().getExtScanRange().getFileScanRange().setSplitSource(tSource);
                TScanRangeLocation location = new TScanRangeLocation();
                location.setBackendId(backend.getId());
                location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
                curLocations.addToLocations(location);
                // So there's only one scan range for each backend.
                // Each backend only starts up one ScanNode instance.
                // However, even one ScanNode instance can provide maximum scanning concurrency.
                scanRangeLocations.add(curLocations);
                setLocationPropertiesIfNecessary(backend, locationType, locationProperties);
                scanBackendIds.add(backend.getId());
            }
        } else {
            List<Split> inputSplits = getSplits(numBackends);
            if (ConnectContext.get().getExecutor() != null) {
                ConnectContext.get().getExecutor().getSummaryProfile().setGetSplitsFinishTime();
            }
            selectedSplitNum = inputSplits.size();
            if (inputSplits.isEmpty() && !isFileStreamType()) {
                return;
            }
            Multimap<Backend, Split> assignment =  backendPolicy.computeScanRangeAssignment(inputSplits);
            for (Backend backend : assignment.keySet()) {
                Collection<Split> splits = assignment.get(backend);
                for (Split split : splits) {
                    scanRangeLocations.add(splitToScanRange(backend, locationProperties, split, pathPartitionKeys));
                    totalFileSize += split.getLength();
                }
                scanBackendIds.add(backend.getId());
            }
        }

        getSerializedTable().ifPresent(params::setSerializedTable);

        if (executor != null) {
            executor.getSummaryProfile().setCreateScanRangeFinishTime();
            if (sessionVariable.showSplitProfileInfo()) {
                executor.getSummaryProfile().setAssignedWeightPerBackend(backendPolicy.getAssignedWeightPerBackend());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("create #{} ScanRangeLocations cost: {} ms",
                    scanRangeLocations.size(), (System.currentTimeMillis() - start));
        }
    }

    private TScanRangeLocations splitToScanRange(
            Backend backend,
            Map<String, String> locationProperties,
            Split split,
            List<String> pathPartitionKeys) throws UserException {
        FileSplit fileSplit = (FileSplit) split;
        TScanRangeLocations curLocations = newLocations();
        // If fileSplit has partition values, use the values collected from hive partitions.
        // Otherwise, use the values in file path.
        boolean isACID = false;
        if (fileSplit instanceof HiveSplit) {
            HiveSplit hiveSplit = (HiveSplit) fileSplit;
            isACID = hiveSplit.isACID();
        }
        List<String> partitionValuesFromPath = fileSplit.getPartitionValues() == null
                ? BrokerUtil.parseColumnsFromPath(fileSplit.getPathString(), pathPartitionKeys,
                false, isACID) : fileSplit.getPartitionValues();

        TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath, pathPartitionKeys);
        TFileCompressType fileCompressType = getFileCompressType(fileSplit);
        rangeDesc.setCompressType(fileCompressType);
        // set file format type, and the type might fall back to native format in setScanParams
        rangeDesc.setFormatType(getFileFormatType());
        setScanParams(rangeDesc, fileSplit);

        curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
        TScanRangeLocation location = new TScanRangeLocation();
        setLocationPropertiesIfNecessary(backend, fileSplit.getLocationType(), locationProperties);
        location.setBackendId(backend.getId());
        location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
        curLocations.addToLocations(location);
        return curLocations;
    }

    private void setLocationPropertiesIfNecessary(Backend selectedBackend, TFileType locationType,
            Map<String, String> locationProperties) throws UserException {
        if (locationType == TFileType.FILE_HDFS || locationType == TFileType.FILE_BROKER) {
            if (!params.isSetHdfsParams()) {
                THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(locationProperties);
                // tHdfsParams.setFsName(getFsName(fileSplit));
                params.setHdfsParams(tHdfsParams);
            }

            if (locationType == TFileType.FILE_BROKER) {
                params.setProperties(locationProperties);

                if (!params.isSetBrokerAddresses()) {
                    List<FsBroker> brokers;
                    if (brokerName != null) {
                        brokers = Env.getCurrentEnv().getBrokerMgr().getBrokers(brokerName);
                    } else {
                        brokers = Env.getCurrentEnv().getBrokerMgr().getAllBrokers();
                    }
                    if (brokers == null || brokers.isEmpty()) {
                        throw new UserException("No alive broker.");
                    }
                    Collections.shuffle(brokers);
                    for (FsBroker broker : brokers) {
                        if (broker.isAlive) {
                            params.addToBrokerAddresses(new TNetworkAddress(broker.host, broker.port));
                        }
                    }
                    if (params.getBrokerAddresses().isEmpty()) {
                        throw new UserException("No alive broker.");
                    }
                }
            }
        } else if ((locationType == TFileType.FILE_S3 || locationType == TFileType.FILE_LOCAL)
                && !params.isSetProperties()) {
            params.setProperties(locationProperties);
        }

        if (!params.isSetFileType()) {
            params.setFileType(locationType);
        }
    }

    private TScanRangeLocations newLocations() {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);
        return locations;
    }

    private TFileRangeDesc createFileRangeDesc(FileSplit fileSplit, List<String> columnsFromPath,
                                               List<String> columnsFromPathKeys) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        // fileSize only be used when format is orc or parquet and TFileType is broker
        // When TFileType is other type, it is not necessary
        rangeDesc.setFileSize(fileSplit.getFileLength());
        rangeDesc.setColumnsFromPath(columnsFromPath);
        rangeDesc.setColumnsFromPathKeys(columnsFromPathKeys);

        rangeDesc.setFileType(fileSplit.getLocationType());
        rangeDesc.setPath(fileSplit.getPath().toStorageLocation().toString());
        if (fileSplit.getLocationType() == TFileType.FILE_HDFS) {
            URI fileUri = fileSplit.getPath().getPath().toUri();
            rangeDesc.setFsName(fileUri.getScheme() + "://" + fileUri.getAuthority());
        }
        rangeDesc.setModificationTime(fileSplit.getModificationTime());
        return rangeDesc;
    }

    // To Support Hive 1.x orc internal column name like (_col0, _col1, _col2...)
    // We need to save mapping from slot name to schema position
    protected void genSlotToSchemaIdMapForOrc() {
        Preconditions.checkNotNull(params);
        List<Column> baseSchema = desc.getTable().getBaseSchema();
        Map<String, Integer> columnNameToPosition = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            int idx = 0;
            for (Column col : baseSchema) {
                if (col.getName().equals(slot.getColumn().getName())) {
                    columnNameToPosition.put(col.getName(), idx);
                    break;
                }
                idx += 1;
            }
        }
        params.setSlotNameToSchemaPos(columnNameToPosition);
    }

    @Override
    public int getScanRangeNum() {
        Preconditions.checkNotNull(scanRangeLocations);
        int i = 0;
        for (TScanRangeLocations tScanRangeLocations : scanRangeLocations) {
            TScanRange tScanRange = tScanRangeLocations.getScanRange();
            TFileScanRange tFileScanRange = tScanRange.getExtScanRange().getFileScanRange();
            i += tFileScanRange.getRangesSize();
        }
        return i;
    }

    @Override
    public int getNumInstances() {
        if (sessionVariable.isIgnoreStorageDataDistribution()) {
            return sessionVariable.getParallelExecInstanceNum();
        }
        return scanRangeLocations.size();
    }

    // Return true if this is a TFileType.FILE_STREAM type.
    // Currently, only TVFScanNode may be TFileType.FILE_STREAM type.
    protected boolean isFileStreamType() throws UserException {
        return false;
    }

    protected abstract TFileFormatType getFileFormatType() throws UserException;

    protected TFileCompressType getFileCompressType(FileSplit fileSplit) throws UserException {
        return Util.inferFileCompressTypeByPath(fileSplit.getPathString());
    }

    protected TFileAttributes getFileAttributes() throws UserException {
        throw new NotImplementedException("getFileAttributes is not implemented.");
    }

    protected abstract List<String> getPathPartitionKeys() throws UserException;

    protected abstract TableIf getTargetTable() throws UserException;

    // TODO: Rename this method when Metadata Service (MS) integration is complete.
    // The current name "getLocationProperties" is a placeholder and may not reflect
    // the new structure of storage parameters expected from MS.
    protected abstract Map<String, String> getLocationProperties() throws UserException;

    @Override
    public void stop() {
        if (splitAssignment != null) {
            splitAssignment.stop();
            SplitSourceManager manager = Env.getCurrentEnv().getSplitSourceManager();
            for (Long sourceId : splitAssignment.getSources()) {
                manager.removeSplitSource(sourceId);
            }
        }
    }

    public void setQueryTableSnapshot(TableSnapshot tableSnapshot) {
        this.tableSnapshot = tableSnapshot;
    }

    public TableSnapshot getQueryTableSnapshot() {
        TableSnapshot snapshot = desc.getRef().getTableSnapshot();
        if (snapshot != null) {
            return snapshot;
        }
        return this.tableSnapshot;
    }

    public void setScanParams(TableScanParams scanParams) {
        this.scanParams = scanParams;
    }

    public TableScanParams getScanParams() {
        TableScanParams scan = desc.getRef().getScanParams();
        if (scan != null) {
            return scan;
        }
        return this.scanParams;
    }

    /**
     * The real file split size is determined by:
     * 1. If user specify the split size in session variable `file_split_size`, use user specified value.
     * 2. Otherwise, use the max value of DEFAULT_SPLIT_SIZE and block size.
     * @param blockSize, got from file system, eg, hdfs
     * @return the real file split size
     */
    protected long getRealFileSplitSize(long blockSize) {
        long realSplitSize = sessionVariable.getFileSplitSize();
        if (realSplitSize <= 0) {
            realSplitSize = Math.max(DEFAULT_SPLIT_SIZE, blockSize);
        }
        return realSplitSize;
    }
}
