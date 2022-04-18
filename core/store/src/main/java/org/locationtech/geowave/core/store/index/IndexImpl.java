/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.PartitionBinningStrategy;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DuplicateEntryCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic;
import com.google.common.collect.Lists;

/**
 * This class fully describes everything necessary to index data within GeoWave. The key components
 * are the indexing strategy and the common index model.
 */
public class IndexImpl implements Index, DefaultStatisticsProvider {
  protected NumericIndexStrategy indexStrategy;
  protected CommonIndexModel indexModel;

  public IndexImpl() {}

  public IndexImpl(final NumericIndexStrategy indexStrategy, final CommonIndexModel indexModel) {
    this.indexStrategy = indexStrategy;
    this.indexModel = indexModel;
  }

  @Override
  public NumericIndexStrategy getIndexStrategy() {
    return indexStrategy;
  }

  @Override
  public CommonIndexModel getIndexModel() {
    return indexModel;
  }

  @Override
  public String getName() {
    return indexStrategy.getId() + "_" + indexModel.getId();
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final IndexImpl other = (IndexImpl) obj;
    return getName().equals(other.getName());
  }

  @Override
  public byte[] toBinary() {
    final byte[] indexStrategyBinary = PersistenceUtils.toBinary(indexStrategy);
    final byte[] indexModelBinary = PersistenceUtils.toBinary(indexModel);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            indexStrategyBinary.length
                + indexModelBinary.length
                + VarintUtils.unsignedIntByteLength(indexStrategyBinary.length));
    VarintUtils.writeUnsignedInt(indexStrategyBinary.length, buf);
    buf.put(indexStrategyBinary);
    buf.put(indexModelBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int indexStrategyLength = VarintUtils.readUnsignedInt(buf);
    final byte[] indexStrategyBinary = ByteArrayUtils.safeRead(buf, indexStrategyLength);

    indexStrategy = (NumericIndexStrategy) PersistenceUtils.fromBinary(indexStrategyBinary);

    final byte[] indexModelBinary = new byte[buf.remaining()];
    buf.get(indexModelBinary);
    indexModel = (CommonIndexModel) PersistenceUtils.fromBinary(indexModelBinary);
  }

  @Override
  public List<Statistic<? extends StatisticValue<?>>> getDefaultStatistics() {
    List<Statistic<? extends StatisticValue<?>>> statistics = Lists.newArrayListWithCapacity(6);
    IndexMetaDataSetStatistic metadata =
        new IndexMetaDataSetStatistic(getName(), indexStrategy.createMetaData());
    metadata.setBinningStrategy(new DataTypeBinningStrategy());
    metadata.setInternal();
    statistics.add(metadata);

    DuplicateEntryCountStatistic duplicateCounts = new DuplicateEntryCountStatistic(getName());
    duplicateCounts.setBinningStrategy(new DataTypeBinningStrategy());
    duplicateCounts.setInternal();
    statistics.add(duplicateCounts);

    PartitionsStatistic partitions = new PartitionsStatistic(getName());
    partitions.setBinningStrategy(new DataTypeBinningStrategy());
    partitions.setInternal();
    statistics.add(partitions);

    DifferingVisibilityCountStatistic differingFieldVisibility =
        new DifferingVisibilityCountStatistic(getName());
    differingFieldVisibility.setBinningStrategy(new DataTypeBinningStrategy());
    differingFieldVisibility.setInternal();
    statistics.add(differingFieldVisibility);

    FieldVisibilityCountStatistic fieldVisibilityCount =
        new FieldVisibilityCountStatistic(getName());
    fieldVisibilityCount.setBinningStrategy(new DataTypeBinningStrategy());
    fieldVisibilityCount.setInternal();
    statistics.add(fieldVisibilityCount);

    RowRangeHistogramStatistic rowRangeHistogram = new RowRangeHistogramStatistic(getName());
    rowRangeHistogram.setBinningStrategy(
        new CompositeBinningStrategy(
            new DataTypeBinningStrategy(),
            new PartitionBinningStrategy()));
    rowRangeHistogram.setInternal();
    statistics.add(rowRangeHistogram);

    return statistics;
  }
}
