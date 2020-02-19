/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.partitioner;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.model.IndexModelBuilder;
import org.locationtech.geowave.analytic.model.SpatialIndexModelBuilder;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.PartitionParameters;
import org.locationtech.geowave.analytic.param.PartitionParameters.Partition;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexImpl;

/**
 * Basic support class for Partitioners (e.g {@link Paritioner}
 *
 * @param <T>
 */
public abstract class AbstractPartitioner<T> implements Partitioner<T> {
  /** */
  private static final long serialVersionUID = 1L;

  private transient Index index = null;
  private double[] distancePerDimension = null;
  private double precisionFactor = 1.0;

  public AbstractPartitioner() {}

  public AbstractPartitioner(
      final CommonIndexModel indexModel,
      final double[] distancePerDimension) {
    super();
    this.distancePerDimension = distancePerDimension;
    this.initIndex(indexModel, distancePerDimension);
  }

  public AbstractPartitioner(final double[] distancePerDimension) {
    super();
    this.distancePerDimension = distancePerDimension;
  }

  protected double[] getDistancePerDimension() {
    return distancePerDimension;
  }

  protected Index getIndex() {
    return index;
  }

  @Override
  public List<PartitionData> getCubeIdentifiers(final T entry) {
    final Set<PartitionData> partitionIdSet = new HashSet<>();

    final NumericDataHolder numericData = getNumericData(entry);
    if (numericData == null) {
      return Collections.emptyList();
    }
    addPartitions(
        partitionIdSet,
        getIndex().getIndexStrategy().getInsertionIds(numericData.primary),
        true);

    for (final MultiDimensionalNumericData expansionData : numericData.expansion) {
      addPartitions(
          partitionIdSet,
          getIndex().getIndexStrategy().getInsertionIds(expansionData),
          false);
    }
    return new ArrayList<>(partitionIdSet);
  }

  @Override
  public void partition(final T entry, final PartitionDataCallback callback) throws Exception {
    final NumericDataHolder numericData = getNumericData(entry);
    if (numericData == null) {
      return;
    }
    final InsertionIds primaryIds =
        getIndex().getIndexStrategy().getInsertionIds(numericData.primary);
    for (final SinglePartitionInsertionIds partitionInsertionIds : primaryIds.getPartitionKeys()) {
      for (final byte[] sortKey : partitionInsertionIds.getSortKeys()) {
        callback.partitionWith(
            new PartitionData(
                new ByteArray(partitionInsertionIds.getPartitionKey()),
                new ByteArray(sortKey),
                true));
      }
    }

    for (final MultiDimensionalNumericData expansionData : numericData.expansion) {
      final InsertionIds expansionIds =
          getIndex().getIndexStrategy().getInsertionIds(expansionData);
      for (final SinglePartitionInsertionIds partitionInsertionIds : expansionIds.getPartitionKeys()) {
        for (final byte[] sortKey : partitionInsertionIds.getSortKeys()) {
          callback.partitionWith(
              new PartitionData(
                  new ByteArray(partitionInsertionIds.getPartitionKey()),
                  new ByteArray(sortKey),
                  false));
        }
      }
    }
  }

  protected static class NumericDataHolder {
    MultiDimensionalNumericData primary;
    MultiDimensionalNumericData[] expansion;
  }

  protected abstract NumericDataHolder getNumericData(final T entry);

  public MultiDimensionalNumericData getRangesForPartition(final PartitionData partitionData) {
    return index.getIndexStrategy().getRangeForId(
        partitionData.getPartitionKey().getBytes(),
        partitionData.getSortKey().getBytes());
  }

  protected void addPartitions(
      final Set<PartitionData> masterList,
      final InsertionIds insertionIds,
      final boolean isPrimary) {
    for (final SinglePartitionInsertionIds partitionInsertionIds : insertionIds.getPartitionKeys()) {
      for (final byte[] sortKey : partitionInsertionIds.getSortKeys()) {
        masterList.add(
            new PartitionData(
                new ByteArray(partitionInsertionIds.getPartitionKey()),
                new ByteArray(sortKey),
                isPrimary));
      }
    }
  }

  private static double[] getDistances(final ScopedJobConfiguration config) {
    final String distances =
        config.getString(PartitionParameters.Partition.DISTANCE_THRESHOLDS, "0.000001");

    final String distancesArray[] = distances.split(",");
    final double[] distancePerDimension = new double[distancesArray.length];
    {
      int i = 0;
      for (final String eachDistance : distancesArray) {
        distancePerDimension[i++] = Double.valueOf(eachDistance);
      }
    }
    return distancePerDimension;
  }

  @Override
  public void initialize(final JobContext context, final Class<?> scope) throws IOException {
    initialize(new ScopedJobConfiguration(context.getConfiguration(), scope));
  }

  public void initialize(final ScopedJobConfiguration config) throws IOException {

    distancePerDimension = getDistances(config);

    this.precisionFactor = config.getDouble(Partition.PARTITION_PRECISION, 1.0);

    if ((precisionFactor < 0) || (precisionFactor > 1.0)) {
      throw new IllegalArgumentException(
          String.format("Precision value must be between 0 and 1: %.6f", precisionFactor));
    }

    try {
      final IndexModelBuilder builder =
          config.getInstance(
              CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
              IndexModelBuilder.class,
              SpatialIndexModelBuilder.class);

      final CommonIndexModel model = builder.buildModel();
      if (model.getDimensions().length > distancePerDimension.length) {
        final double[] newDistancePerDimension = new double[model.getDimensions().length];
        for (int j = 0; j < newDistancePerDimension.length; j++) {
          newDistancePerDimension[j] =
              distancePerDimension[j < distancePerDimension.length ? j
                  : (distancePerDimension.length - 1)];
        }
        distancePerDimension = newDistancePerDimension;
      }
      this.initIndex(model, distancePerDimension);

    } catch (InstantiationException | IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setup(
      final PropertyManagement runTimeProperties,
      final Class<?> scope,
      final Configuration configuration) {
    final ParameterEnum[] params =
        new ParameterEnum[] {
            CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
            PartitionParameters.Partition.DISTANCE_THRESHOLDS,
            Partition.PARTITION_PRECISION};
    runTimeProperties.setConfig(params, configuration, scope);
  }

  protected void initIndex(
      final CommonIndexModel indexModel,
      final double[] distancePerDimensionForIndex) {

    // truncating to lower precision
    final NumericDimensionField<?>[] dimensions = indexModel.getDimensions();

    int totalRequestedPrecision = 0;
    final int[] dimensionPrecision = new int[indexModel.getDimensions().length];
    for (int i = 0; i < dimensionPrecision.length; i++) {
      final double distance = distancePerDimensionForIndex[i] * 2.0; // total
      // width...(radius)
      // adjust by precision factory (0 to 1.0)
      dimensionPrecision[i] =
          (int) (precisionFactor
              * Math.abs((int) (Math.log(dimensions[i].getRange() / distance) / Math.log(2))));

      totalRequestedPrecision += dimensionPrecision[i];
    }
    if (totalRequestedPrecision > 63) {
      final double rescale = 63.0 / totalRequestedPrecision;
      for (int i = 0; i < dimensionPrecision.length; i++) {
        dimensionPrecision[i] = (int) (rescale * dimensionPrecision[i]);
      }
    }

    final TieredSFCIndexStrategy indexStrategy =
        TieredSFCIndexFactory.createSingleTierStrategy(
            indexModel.getDimensions(),
            dimensionPrecision,
            SFCType.HILBERT);

    // Not relevant since this is a single tier strategy.
    // For now, just setting to a non-zero reasonable value
    indexStrategy.setMaxEstimatedDuplicateIdsPerDimension(2);

    index = new IndexImpl(indexStrategy, indexModel);
  }

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    return Arrays.asList(
        new ParameterEnum<?>[] {
            CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
            PartitionParameters.Partition.DISTANCE_THRESHOLDS,
            Partition.PARTITION_PRECISION});
  }

  private void writeObject(final ObjectOutputStream stream) throws IOException {
    final byte[] indexData = PersistenceUtils.toBinary(this.index);
    stream.writeInt(indexData.length);
    stream.write(indexData);
    stream.writeDouble(precisionFactor);
    stream.writeInt(distancePerDimension.length);
    for (final double v : distancePerDimension) {
      stream.writeDouble(v);
    }
  }

  private void readObject(final java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    final byte[] indexData = new byte[stream.readInt()];
    stream.readFully(indexData);
    index = (Index) PersistenceUtils.fromBinary(indexData);
    precisionFactor = stream.readDouble();
    distancePerDimension = new double[stream.readInt()];
    for (int i = 0; i < distancePerDimension.length; i++) {
      distancePerDimension[i] = stream.readDouble();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Arrays.hashCode(distancePerDimension);
    result = (prime * result) + ((index == null) ? 0 : index.hashCode());
    long temp;
    temp = Double.doubleToLongBits(precisionFactor);
    result = (prime * result) + (int) (temp ^ (temp >>> 32));
    return result;
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
    final AbstractPartitioner other = (AbstractPartitioner) obj;
    if (!Arrays.equals(distancePerDimension, other.distancePerDimension)) {
      return false;
    }
    if (index == null) {
      if (other.index != null) {
        return false;
      }
    } else if (!index.equals(other.index)) {
      return false;
    }
    if (Double.doubleToLongBits(precisionFactor) != Double.doubleToLongBits(
        other.precisionFactor)) {
      return false;
    }
    return true;
  }
}
