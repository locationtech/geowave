/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.index;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

/**
 * This class is responsible for maintaining all unique Partition IDs that are being used within a
 * data set.
 */
public class PartitionsStatistic extends IndexStatistic<PartitionsStatistic.PartitionsValue> {
  public static final IndexStatisticType<PartitionsValue> STATS_TYPE =
      new IndexStatisticType<>("PARTITIONS");

  public PartitionsStatistic() {
    super(STATS_TYPE);
  }

  public PartitionsStatistic(final String indexName) {
    super(STATS_TYPE, indexName);
  }

  @Override
  public String getDescription() {
    return "Maintains a set of all unique partition IDs.";
  }

  @Override
  public PartitionsValue createEmpty() {
    return new PartitionsValue(this);
  }

  public static class PartitionsValue extends StatisticValue<Set<ByteArray>> implements
      StatisticsIngestCallback {

    private Set<ByteArray> partitions = new HashSet<>();

    public PartitionsValue() {
      this(null);
    }

    public PartitionsValue(Statistic<?> statistic) {
      super(statistic);
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge instanceof PartitionsValue) {
        partitions.addAll(((PartitionsValue) merge).partitions);
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      for (final GeoWaveRow kv : rows) {
        partitions.add(getPartitionKey(kv.getPartitionKey()));
      }
    }

    @Override
    public Set<ByteArray> getValue() {
      return partitions;
    }

    @Override
    public byte[] toBinary() {
      if (!partitions.isEmpty()) {
        // we know each partition is constant size, so start with the size
        // of the partition keys
        final ByteArray first = partitions.iterator().next();
        if ((first != null) && (first.getBytes() != null)) {
          final ByteBuffer buffer =
              ByteBuffer.allocate((first.getBytes().length * partitions.size()) + 1);
          buffer.put((byte) first.getBytes().length);
          for (final ByteArray e : partitions) {
            buffer.put(e.getBytes());
          }
          return buffer.array();
        }
      }
      return new byte[0];
    }

    @Override
    public void fromBinary(byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      partitions = new HashSet<>();
      if (buffer.remaining() > 0) {
        final int partitionKeySize = unsignedToBytes(buffer.get());
        if (partitionKeySize > 0) {
          final int numPartitions = buffer.remaining() / partitionKeySize;
          for (int i = 0; i < numPartitions; i++) {
            final byte[] partition = ByteArrayUtils.safeRead(buffer, partitionKeySize);
            partitions.add(new ByteArray(partition));
          }
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (ByteArray partition : partitions) {
        sb.append(Arrays.toString(partition.getBytes())).append(",");
      }
      if (partitions.size() > 0) {
        // Remove last comma
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append("]");
      return sb.toString();
    }
  }

  protected static ByteArray getPartitionKey(final byte[] partitionBytes) {
    return ((partitionBytes == null) || (partitionBytes.length == 0)) ? null
        : new ByteArray(partitionBytes);
  }

  public static int unsignedToBytes(final byte b) {
    return b & 0xFF;
  }
}
