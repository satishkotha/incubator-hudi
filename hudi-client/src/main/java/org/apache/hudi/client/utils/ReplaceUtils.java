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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.WriteStatus;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReplaceUtils {

  public static Map<String, List<String>> getPartitionToReplaceFileIdMap(JavaRDD<WriteStatus> writeStatuses) {
    // gather all fileIds replaced to make it easy to exclude these file groups on subsequent actions
    return writeStatuses.filter(st -> st.isReplaced())
        .mapToPair(status -> new Tuple2<>(status.getPartitionPath(), status.getFileId())).groupByKey()
        .mapToPair(iterable -> new Tuple2<>(iterable._1, StreamSupport.stream(iterable._2.spliterator(), false).collect(Collectors.toList())))
        .collectAsMap();
  }
}
