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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceStat;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Helper class to generate compaction plan from FileGroup/FileSlice abstraction.
 */
public class CommitUtils {

  private static final Logger LOG = LogManager.getLogger(CommitUtils.class);

  public static HoodieCommitMetadata buildCommitMetadata(List<HoodieWriteStat> writeStats,
                                                         Option<Map<String, String>> extraMetadata,
                                                         WriteOperationType operationType,
                                                         String schemaToStoreInCommit) {

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    int numReplaceStats = 0;
    int numWriteStats = 0;
    for (HoodieWriteStat writeStat : writeStats) {
      String partition = writeStat.getPartitionPath();

      if (writeStat instanceof HoodieReplaceStat) {
        numReplaceStats++;
        commitMetadata.addReplaceStat(partition, (HoodieReplaceStat) writeStat);
      } else {
        numWriteStats++;
        commitMetadata.addWriteStat(partition, writeStat);
      }
    }

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(commitMetadata::addMetadata);
    }
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schemaToStoreInCommit);
    commitMetadata.setOperationType(operationType);

    LOG.info("Creating commit metadata for " + operationType + " numWriteStats:" + numWriteStats
        + "numReplaceStats:" + numReplaceStats);
    return commitMetadata;
  }
}
