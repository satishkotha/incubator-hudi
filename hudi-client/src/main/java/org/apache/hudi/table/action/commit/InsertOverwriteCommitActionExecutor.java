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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.avro.model.HoodieReplaceMetadata;
import org.apache.hudi.client.utils.ReplaceUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InsertOverwriteCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends CommitActionExecutor<T> {

  private final JavaRDD<HoodieRecord<T>> inputRecordsRDD;

  public InsertOverwriteCommitActionExecutor(JavaSparkContext jsc,
                                             HoodieWriteConfig config, HoodieTable table,
                                             String instantTime, JavaRDD<HoodieRecord<T>> inputRecordsRDD) {
    super(jsc, config, table, instantTime, WriteOperationType.INSERT_OVERWRITE);
    this.inputRecordsRDD = inputRecordsRDD;
  }

  @Override
  public HoodieWriteMetadata execute() {
    return WriteHelper.write(instantTime, inputRecordsRDD, jsc, (HoodieTable<T>) table,
        config.shouldCombineBeforeInsert(), config.getInsertShuffleParallelism(), this, false);
  }

  @Override
  protected Partitioner getPartitioner(WorkloadProfile profile) {
    return new InsertOverwritePartitioner<>(profile, jsc, table, config);
  }

  @Override
  protected void commitOnAutoCommit(HoodieWriteMetadata result) {
    // gather all fileIds replaced and save it as action to the timeline.
    // Commit for insert overwrite is a 2 step process
    // 1. write instant.replace file
    // 2. write instant.[delta]commit file
    // replace file should not be used in metadata if theres no corresponding commit/deltacommit file.
    Map<String, List<String>> partitionToReplacedFiles = ReplaceUtils.getPartitionToReplaceFileIdMap(result.getWriteStatuses());
    if (!partitionToReplacedFiles.isEmpty()) {
      try {
        HoodieReplaceMetadata replaceMetadata = new HoodieReplaceMetadata();
        replaceMetadata.setPolicy(HoodieTimeline.REPLACE_ACTION);
        replaceMetadata.setTotalFilesReplaced(partitionToReplacedFiles.values().stream().mapToInt(x -> x.size()).sum());
        replaceMetadata.setPartitionMetadata(partitionToReplacedFiles);
        replaceMetadata.setVersion(1);
        HoodieInstant replaceInstant = new HoodieInstant(true, HoodieTimeline.REPLACE_ACTION, instantTime);
        table.getActiveTimeline().createNewInstant(replaceInstant);
        table.getActiveTimeline().saveAsComplete(replaceInstant,
            TimelineMetadataUtils.serializeReplaceMetadata(replaceMetadata));
      } catch (IOException e) {
        throw new HoodieIOException("error writing replace metadata", e);
      }

    }

    super.commitOnAutoCommit(result);
  }
}
