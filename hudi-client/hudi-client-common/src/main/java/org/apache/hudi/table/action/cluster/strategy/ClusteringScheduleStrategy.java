/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClusteringScheduleStrategy<T extends HoodieRecordPayload,I,K,O> implements Serializable {
  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_IO_WRITE_MB = "TOTAL_IO_WRITE_MB";
  public static final String TOTAL_IO_MB = "TOTAL_IO_MB";
  public static final String TOTAL_LOG_FILE_SIZE = "TOTAL_LOG_FILES_SIZE";
  public static final String TOTAL_LOG_FILES = "TOTAL_LOG_FILES";

  private static final String CLUSTERING_PLAN_VERSION = "CLUSTERING_PLAN_VERSION";
  private static int CLUSTERING_PLAN_VERSION_1 = 1;

  private final HoodieTable<T,I,K,O> hoodieTable;
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig writeConfig;

  public ClusteringScheduleStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
    this.hoodieTable = table;
    this.engineContext = engineContext;
  }

  public HoodieClusteringPlan generateClusteringPlan(Stream<FileSlice> fileSlices) {
    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(getClass().getName())
        .setStrategyParams(getStrategyParams())
        .build();

    List<HoodieClusteringGroup> clusteringGroups = buildClusteringGroups(fileSlices);
    return HoodieClusteringPlan.newBuilder()
        .setStrategy(strategy)
        .setInputGroups(clusteringGroups)
        .setExtraMetadata(getExtraMetadata())
        .setVersion(getPlanVersion())
        .build();
  }

  protected  List<HoodieClusteringGroup> buildClusteringGroups(Stream<FileSlice> fileSlices) {
    Map<String, List<FileSlice>> partitionToFileSlices = fileSlices.collect(Collectors.groupingBy(FileSlice::getPartitionPath));
    return engineContext.flatMap(new ArrayList<>(partitionToFileSlices.values()),
        this::buildClusteringGroupsForPartition, partitionToFileSlices.size());
  }

  /**
   * Distribute fileslices of same partition into multiple groups based on strategy specific parameters.
   * Default implementation here groups all fileslices into one group.
   */
  protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(List<FileSlice> fileSlices) {
    List<List<FileSlice>> fileSliceGroups = new ArrayList<>();
    List<FileSlice> currentGroup = new ArrayList<>();
    int totalSizeSoFar = 0;
    for (FileSlice currentSlice : fileSlices) {
      // check if max size is reached and create new group, if needed.
      totalSizeSoFar += currentSlice.getBaseFile().isPresent() ? currentSlice.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize();
      if (totalSizeSoFar >= writeConfig.getClusteringMaxDataSizeInGroup() && !currentGroup.isEmpty()) {
        fileSliceGroups.add(currentGroup);
        currentGroup = new ArrayList<>();
      }
      currentGroup.add(currentSlice);
    }
    if (!currentGroup.isEmpty()) {
      fileSliceGroups.add(currentGroup);
    }

    return fileSliceGroups.stream().map(fileSliceGroup -> HoodieClusteringGroup.newBuilder()
        .setSlices(getFileSliceInfo(fileSliceGroup))
        .setMetrics(buildMetrics(fileSliceGroup))
        .build());
  }

  protected Map<String, String> getExtraMetadata() {
    return Collections.emptyMap();
  }

  protected int getPlanVersion() {
    return CLUSTERING_PLAN_VERSION_1;
  }

  private List<HoodieSliceInfo> getFileSliceInfo(List<FileSlice> slices) {
    return slices.stream().map(slice -> new HoodieSliceInfo().newBuilder()
        .setPartitionPath(slice.getPartitionPath())
        .setFileId(slice.getFileId())
        .setDataFilePath(slice.getBaseFile().map(BaseFile::getPath).orElse(""))
        .setDeltaFilePaths(slice.getLogFiles().map(f -> f.getPath().getName()).collect(Collectors.toList()))
        .setBootstrapFilePath(slice.getBaseFile().map(bf -> bf.getBootstrapBaseFile().map(bbf -> bbf.getPath()).orElse("")).orElse(""))
        .build()).collect(Collectors.toList());
  }

  private Map<String, Double> buildMetrics(List<FileSlice> fileSlices) {
    int numLogFiles = 0;
    long totalLogFileSize = 0;
    long totalIORead = 0;
    long totalIOWrite = 0;
    long totalIO = 0;

    for (FileSlice slice : fileSlices) {
      numLogFiles +=  slice.getLogFiles().count();
      // Total size of all the log files
      totalLogFileSize += slice.getLogFiles().map(HoodieLogFile::getFileSize).filter(size -> size >= 0)
          .reduce(Long::sum).orElse(0L);
      // Total read will be the base file + all the log files
      totalIORead =
          FSUtils.getSizeInMB((slice.getBaseFile().isPresent() ? slice.getBaseFile().get().getFileSize() : 0L) + totalLogFileSize);
      // Total write will be similar to the size of the base file
      totalIOWrite =
          FSUtils.getSizeInMB(slice.getBaseFile().isPresent() ? slice.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize());
      // Total IO will the the IO for read + write
      totalIO = totalIORead + totalIOWrite;
    }

    Map<String, Double> metrics = new HashMap<>();
    metrics.put(TOTAL_IO_READ_MB, (double) totalIORead);
    metrics.put(TOTAL_IO_WRITE_MB, (double) totalIOWrite);
    metrics.put(TOTAL_IO_MB, (double) totalIO);
    metrics.put(TOTAL_LOG_FILE_SIZE, (double) totalLogFileSize);
    metrics.put(TOTAL_LOG_FILES, (double) numLogFiles);
    return metrics;
  }

  /**
   * Execution of reading inputGroups in {@link HoodieClusteringPlan} and create outputGroups.
   * Note that engineContext is available
   */
  public HoodieWriteMetadata<O> executeClustering(HoodieClusteringPlan plan, String instantTime) {
    List<HoodieWriteStat> writeStats =
        engineContext.flatMap(plan.getInputGroups(), this::executeClusteringForGroup, plan.getInputGroups().size());

    Map<String, List<String>> partitionToReplaceFileIds = getPartitionToFileReplaceStats(plan);

    CommitUtils.buildMetadata(writeStats, partitionToReplaceFileIds, Option.of(getExtraMetadata()),
        WriteOperationType.CLUSTERING, writeConfig.getSchema(), HoodieTimeline.REPLACE_COMMIT_ACTION);

  }

  private Map<String, List<String>> getPartitionToFileReplaceStats(HoodieClusteringPlan plan) {
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    plan.getInputGroups().stream().forEach(clusteringGroup -> {
      clusteringGroup.getSlices().stream().forEach(sliceInfo -> {
        partitionToReplaceFileIds.putIfAbsent(sliceInfo.getPartitionPath(), new ArrayList<>());
        partitionToReplaceFileIds.get(sliceInfo.getPartitionPath()).add(sliceInfo.getFileId());
      });
    });

    return partitionToReplaceFileIds;
  }

  protected Stream<HoodieWriteStat> executeClusteringForGroup(HoodieClusteringGroup clusteringGroup) {
    return null;
  }

  public Map<String, String> getStrategyParams() {
    return new HashMap<String, String>() {{
      put(CLUSTERING_PLAN_VERSION, String.valueOf(CLUSTERING_PLAN_VERSION_1));
    }};
  }

  protected HoodieTable<T,I,K, O> getHoodieTable() {
    return this.hoodieTable;
  }

  protected HoodieEngineContext getEngineContext() {
    return this.engineContext;
  }

  protected HoodieWriteConfig getWriteConfig() {
    return this.writeConfig;
  }
}
