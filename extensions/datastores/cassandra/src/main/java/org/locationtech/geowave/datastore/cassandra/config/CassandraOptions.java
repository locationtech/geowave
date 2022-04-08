/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.LeveledCompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.SizeTieredCompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.TimeWindowCompactionStrategy;

public class CassandraOptions extends BaseDataStoreOptions {
  @Parameter(names = "--batchWriteSize", description = "The number of inserts in a batch write.")
  private int batchWriteSize = 50;

  @Parameter(
      names = "--durableWrites",
      description = "Whether to write to commit log for durability, configured only on creation of new keyspace.",
      arity = 1)
  private boolean durableWrites = true;

  @Parameter(
      names = "--replicas",
      description = "The number of replicas to use when creating a new keyspace.")
  private int replicationFactor = 3;

  @Parameter(
      names = "--gcGraceSeconds",
      description = "The gc_grace_seconds applied to each Cassandra table. Defaults to 10 days and major compaction should be triggered at least as often.")
  private int gcGraceSeconds = 864000;

  @Parameter(
      names = "--compactionStrategy",
      description = "The compaction strategy applied to each Cassandra table. Available options are LeveledCompactionStrategy, SizeTieredCompactionStrategy, or TimeWindowCompactionStrategy.",
      converter = CompactionStrategyConverter.class)
  private CompactionStrategy<?> compactionStrategy = SchemaBuilder.sizeTieredCompactionStrategy();

  @DynamicParameter(
      names = "--tableOptions",
      description = "Any general table options as 'key=value' applied to each Cassandra table.")
  private Map<String, String> tableOptions = new HashMap<>();

  public int getGcGraceSeconds() {
    return gcGraceSeconds;
  }

  public void setGcGraceSeconds(final int gcGraceSeconds) {
    this.gcGraceSeconds = gcGraceSeconds;
  }

  public int getBatchWriteSize() {
    return batchWriteSize;
  }

  public void setBatchWriteSize(final int batchWriteSize) {
    this.batchWriteSize = batchWriteSize;
  }

  public boolean isDurableWrites() {
    return durableWrites;
  }

  public void setDurableWrites(final boolean durableWrites) {
    this.durableWrites = durableWrites;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(final int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public String getCompactionStrategyStr() {
    if (compactionStrategy == null) {
      return null;
    } else {
      if (compactionStrategy instanceof TimeWindowCompactionStrategy) {
        return "TimeWindowCompactionStrategy";
      } else if (compactionStrategy instanceof LeveledCompactionStrategy) {
        return "LeveledCompactionStrategy";
      } else if (compactionStrategy instanceof SizeTieredCompactionStrategy) {
        return "SizeTieredCompactionStrategy";
      }
    }
    return null;
  }

  public void setCompactionStrategyStr(final String compactionStrategyStr) {
    compactionStrategy = convertCompactionStrategy(compactionStrategyStr);
  }

  public CompactionStrategy<?> getCompactionStrategy() {
    return compactionStrategy;
  }

  public void setCompactionStrategy(final CompactionStrategy<?> compactionStrategy) {
    this.compactionStrategy = compactionStrategy;
  }

  public Map<String, String> getTableOptions() {
    return tableOptions;
  }

  public void setTableOptions(final Map<String, String> tableOptions) {
    this.tableOptions = tableOptions;
  }

  @Override
  public boolean isServerSideLibraryEnabled() {
    return false;
  }

  @Override
  protected boolean defaultEnableVisibility() {
    return false;
  }

  public static class CompactionStrategyConverter implements
      IStringConverter<CompactionStrategy<?>> {

    @Override
    public CompactionStrategy<?> convert(final String value) {
      return convertCompactionStrategy(value);
    }
  }

  private static CompactionStrategy<?> convertCompactionStrategy(final String value) {
    if ((value != null) && !value.isEmpty()) {
      final String str = value.trim().toLowerCase();
      switch (str) {
        case "leveledcompactionstrategy":
        case "lcs":
          return SchemaBuilder.leveledCompactionStrategy();
        case "sizetieredcompactionstrategy":
        case "stcs":
          return SchemaBuilder.sizeTieredCompactionStrategy();
        case "timewindowcompactionstrategy":
        case "twcs":
          return SchemaBuilder.timeWindowCompactionStrategy();
      }
      // backup to a more lenient "contains" check as a last resort (because class names contain
      // these strings so in case a Java object gets serialized to a string this will still work
      if (str.contains("leveledcompactionstrategy")) {
        return SchemaBuilder.leveledCompactionStrategy();
      } else if (str.contains("sizetieredcompactionstrategy")) {
        return SchemaBuilder.sizeTieredCompactionStrategy();
      } else if (str.contains("timewindowcompactionstrategy")) {
        return SchemaBuilder.timeWindowCompactionStrategy();

      }
      throw new IllegalArgumentException(
          "Unable to convert '"
              + value
              + "' to compaction strategy. Available options are LeveledCompactionStrategy, SizeTieredCompactionStrategy, or TimeWindowCompactionStrategy.");
    }
    return null;
  }

  public static class SemiColonSplitter implements IParameterSplitter {

    @Override
    public List<String> split(final String value) {
      return Arrays.asList(value.split(";"));
    }
  }
}
