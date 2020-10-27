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

package org.apache.hudi.config;

import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.table.action.cluster.strategy.ClusteringScheduleStrategy;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Clustering specific configs.
 */
public class HoodieClusteringConfig extends DefaultHoodieConfig {

  public static final String CLUSTERING_STRATEGY_CLASS = "hoodie.clustering.strategy.class";
  public static final String DEFAULT_CLUSTERING_STRATEGY_CLASS = ClusteringScheduleStrategy.class.getName();

  // Any strategy specific params can be saved with this prefix
  public static final String CLUSTERING_STRATEGY_PARAM_PREFIX = "hoodie.clustering.strategy.param.";

  public static final String CLUSTERING_PARALLELISM = "hoodie.clustering.parallelism";
  public static final String DEFAULT_CLUSTERING_PARALLELISM = String.valueOf(600);

  // Max amount of data to be included in one group
  public static final String CLUSTERING_MAX_GROUP_SIZE = "hoodie.clustering.max.group.size";
  public static final String DEFAULT_CLUSTERING_MAX_GROUP_SIZE = String.valueOf(2 * 1024 * 1024 * 1024);

  public HoodieClusteringConfig(Properties props) {
    super(props);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder withClusteringStrategyClass(String clusteringStrategyClass) {
      props.setProperty(CLUSTERING_STRATEGY_CLASS, clusteringStrategyClass);
      return this;
    }

    public Builder withClusteringParallelism(String clusteringParallelism) {
      props.setProperty(CLUSTERING_PARALLELISM, clusteringParallelism);
      return this;
    }

    public Builder withClusteringMaxGroupSize(String clusteringMaxGroupSize) {
      props.setProperty(CLUSTERING_MAX_GROUP_SIZE, clusteringMaxGroupSize);
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieClusteringConfig build() {
      HoodieClusteringConfig config = new HoodieClusteringConfig(props);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_PARALLELISM), CLUSTERING_PARALLELISM,
          DEFAULT_CLUSTERING_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_STRATEGY_CLASS),
          CLUSTERING_STRATEGY_CLASS, DEFAULT_CLUSTERING_STRATEGY_CLASS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_MAX_GROUP_SIZE), CLUSTERING_MAX_GROUP_SIZE,
          DEFAULT_CLUSTERING_MAX_GROUP_SIZE);
      return config;
    }
  }
}
