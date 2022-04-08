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
import java.util.List;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsDeleteCallback;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.clearspring.analytics.util.Lists;

/**
 * Maintains metadata about an index. The tracked metadata is provided by the index strategy.
 */
public class IndexMetaDataSetStatistic extends
    IndexStatistic<IndexMetaDataSetStatistic.IndexMetaDataSetValue> {
  public static final IndexStatisticType<IndexMetaDataSetValue> STATS_TYPE =
      new IndexStatisticType<>("INDEX_METADATA");

  private byte[] metadata = null;

  public IndexMetaDataSetStatistic() {
    this(null, Lists.newArrayList());
  }

  public IndexMetaDataSetStatistic(final String indexName) {
    this(indexName, Lists.newArrayList());
  }

  public IndexMetaDataSetStatistic(final String indexName, List<IndexMetaData> baseMetadata) {
    super(STATS_TYPE, indexName);
    this.metadata = PersistenceUtils.toBinary(baseMetadata);
  }

  @Override
  public String getDescription() {
    return "Maintains metadata about an index.";
  }

  @Override
  public IndexMetaDataSetValue createEmpty() {
    IndexMetaDataSetValue value = new IndexMetaDataSetValue(this);
    value.fromBinary(metadata);
    return value;
  }

  @Override
  protected int byteLength() {
    return super.byteLength()
        + metadata.length
        + VarintUtils.unsignedIntByteLength(metadata.length);
  }

  @Override
  protected void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedInt(metadata.length, buffer);
    buffer.put(metadata);
  }

  @Override
  protected void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    metadata = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(metadata);
  }

  public static class IndexMetaDataSetValue extends StatisticValue<List<IndexMetaData>> implements
      StatisticsIngestCallback,
      StatisticsDeleteCallback {

    private List<IndexMetaData> metadata;

    public IndexMetaDataSetValue() {
      this(null);
    }

    public IndexMetaDataSetValue(Statistic<?> statistic) {
      super(statistic);
    }

    public IndexMetaData[] toArray() {
      return metadata.toArray(new IndexMetaData[metadata.size()]);
    }

    @Override
    public void merge(Mergeable merge) {
      if ((merge != null) && (merge instanceof IndexMetaDataSetValue)) {
        for (int i = 0; i < metadata.size(); i++) {
          final IndexMetaData imd = metadata.get(i);
          final IndexMetaData imd2 = ((IndexMetaDataSetValue) merge).metadata.get(i);
          imd.merge(imd2);
        }
      }
    }

    @Override
    public <T> void entryDeleted(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      if (!this.metadata.isEmpty()) {
        final InsertionIds insertionIds = DataStoreUtils.keysToInsertionIds(rows);
        for (final IndexMetaData imd : this.metadata) {
          imd.insertionIdsRemoved(insertionIds);
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      if (!this.metadata.isEmpty()) {
        final InsertionIds insertionIds = DataStoreUtils.keysToInsertionIds(rows);
        for (final IndexMetaData imd : this.metadata) {
          imd.insertionIdsAdded(insertionIds);
        }
      }
    }

    @Override
    public List<IndexMetaData> getValue() {
      return metadata;
    }

    @Override
    public byte[] toBinary() {
      return PersistenceUtils.toBinary(metadata);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void fromBinary(byte[] bytes) {
      metadata = (List) PersistenceUtils.fromBinaryAsList(bytes);
    }
  }
}
