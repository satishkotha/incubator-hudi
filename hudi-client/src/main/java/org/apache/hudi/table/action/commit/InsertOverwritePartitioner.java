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

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Packs incoming records to be inserted into buckets (1 bucket = 1 RDD partition).
 */
public class InsertOverwritePartitioner<T extends HoodieRecordPayload<T>> extends UpsertPartitioner {

  private static final Logger LOG = LogManager.getLogger(InsertOverwritePartitioner.class);

  public InsertOverwritePartitioner(WorkloadProfile profile, JavaSparkContext jsc, HoodieTable<T> table,
                                    HoodieWriteConfig config) {
    super(profile, jsc, table, config);
    assignReplaceBuckets(profile);
  }

  private void assignReplaceBuckets(WorkloadProfile profile) {
    Set<String> partitions = profile.getPartitionPaths();
    for (String partitionPath : partitions) {
      for (String fileId : getAllExistingFileIds(partitionPath)) {
        LOG.info("marking fileId " + fileId + " in partition " + partitionPath + " for replace");
        addBucket(partitionPath, fileId, BucketType.REPLACE);
      }
    }
  }

  private List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all base files as old files
    return table.getBaseFileOnlyView().getAllBaseFiles(partitionPath).map(baseFile -> baseFile.getFileId())
        .collect(Collectors.toList());
  }

  /**
   * Returns a list of small files in the given partition path.
   */
  protected List<SmallFile> getSmallFiles(String partitionPath) {
    // for overwrite, we ignore all existing files. So dont consider any file to be smallFiles
    return Collections.emptyList();
  }
}
