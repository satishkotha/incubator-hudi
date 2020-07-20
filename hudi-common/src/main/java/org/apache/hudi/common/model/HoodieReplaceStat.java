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

package org.apache.hudi.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Collections;
import java.util.List;

/**
 * Statistics about a single Hoodie replace operation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieReplaceStat extends HoodieWriteStat {

  // records from the 'getFileId()' can be written to multiple new file groups. This list tracks all new fileIds
  private List<String> newFileIds;

  public HoodieReplaceStat() {
    // called by jackson json lib
    newFileIds = Collections.emptyList();
  }

  public void setNewFileIds(List<String> fileIds) {
    this.newFileIds = fileIds;
  }

  public List<String> getNewFileIds() {
    return Collections.unmodifiableList(this.newFileIds);
  }

  @Override
  public String toString() {
    return "HoodieReplaceStat{fileId='" + getFileId() + '\'' + ", path='" + getPath() + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieReplaceStat that = (HoodieReplaceStat) o;
    return getPath().equals(that.getPath());

  }

  @Override
  public int hashCode() {
    return getPath().hashCode();
  }
}
