/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.partitioner;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.model.IndexModelBuilder;
import mil.nga.giat.geowave.analytic.model.SpatialIndexModelBuilder;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Basic support class for Partitioners (e.g {@link Paritioner}
 * 
 * @param <T>
 */
public abstract class AbstractPartitioner<T> implements
		Partitioner<T>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient PrimaryIndex index = null;
	private double[] distancePerDimension = null;
	private double precisionFactor = 1.0;

	public AbstractPartitioner() {}

	public AbstractPartitioner(
			final CommonIndexModel indexModel,
			final double[] distancePerDimension ) {
		super();
		this.distancePerDimension = distancePerDimension;
		this.initIndex(
				indexModel,
				distancePerDimension);

	}

	public AbstractPartitioner(
			final double[] distancePerDimension ) {
		super();
		this.distancePerDimension = distancePerDimension;
	}

	protected double[] getDistancePerDimension() {
		return distancePerDimension;
	}

	protected PrimaryIndex getIndex() {
		return index;
	}

	@Override
	public List<PartitionData> getCubeIdentifiers(
			final T entry ) {
		final Set<PartitionData> partitionIdSet = new HashSet<PartitionData>();

		final NumericDataHolder numericData = getNumericData(entry);
		if (numericData == null) {
			return Collections.emptyList();
		}
		addPartitions(
				partitionIdSet,
				getIndex().getIndexStrategy().getInsertionIds(
						numericData.primary),
				true);

		for (final MultiDimensionalNumericData expansionData : numericData.expansion) {
			addPartitions(
					partitionIdSet,
					getIndex().getIndexStrategy().getInsertionIds(
							expansionData),
					false);
		}
		return new ArrayList<PartitionData>(
				partitionIdSet);
	}

	@Override
	public void partition(
			final T entry,
			final PartitionDataCallback callback )
			throws Exception {
		final NumericDataHolder numericData = getNumericData(entry);
		if (numericData == null) {
			return;
		}
		for (final ByteArrayId addId : getIndex().getIndexStrategy().getInsertionIds(
				numericData.primary)) {
			callback.partitionWith(new PartitionData(
					addId,
					true));
		}

		for (final MultiDimensionalNumericData expansionData : numericData.expansion) {
			for (final ByteArrayId addId : getIndex().getIndexStrategy().getInsertionIds(
					expansionData)) {
				callback.partitionWith(new PartitionData(
						addId,
						false));
			}
		}
	}

	protected static class NumericDataHolder
	{
		MultiDimensionalNumericData primary;
		MultiDimensionalNumericData[] expansion;
	}

	protected abstract NumericDataHolder getNumericData(
			final T entry );

	public MultiDimensionalNumericData getRangesForPartition(
			final PartitionData partitionData ) {
		return index.getIndexStrategy().getRangeForId(
				partitionData.getId());
	}

	protected void addPartitions(
			final Set<PartitionData> masterList,
			final List<ByteArrayId> addList,
			final boolean isPrimary ) {
		for (final ByteArrayId addId : addList) {
			masterList.add(new PartitionData(
					addId,
					isPrimary));
		}
	}

	private static double[] getDistances(
			final ScopedJobConfiguration config ) {
		final String distances = config.getString(
				PartitionParameters.Partition.DISTANCE_THRESHOLDS,
				"0.000001");

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
	public void initialize(
			final JobContext context,
			final Class<?> scope )
			throws IOException {
		initialize(new ScopedJobConfiguration(
				context.getConfiguration(),
				scope));
	}

	public void initialize(
			final ScopedJobConfiguration config )
			throws IOException {

		distancePerDimension = getDistances(config);

		this.precisionFactor = config.getDouble(
				Partition.PARTITION_PRECISION,
				1.0);

		if ((precisionFactor < 0) || (precisionFactor > 1.0)) {
			throw new IllegalArgumentException(
					String.format(
							"Precision value must be between 0 and 1: %.6f",
							precisionFactor));
		}

		try {
			final IndexModelBuilder builder = config.getInstance(
					CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
					IndexModelBuilder.class,
					SpatialIndexModelBuilder.class);

			final CommonIndexModel model = builder.buildModel();
			if (model.getDimensions().length > distancePerDimension.length) {
				final double[] newDistancePerDimension = new double[model.getDimensions().length];
				for (int j = 0; j < newDistancePerDimension.length; j++) {
					newDistancePerDimension[j] = distancePerDimension[j < distancePerDimension.length ? j
							: (distancePerDimension.length - 1)];
				}
				distancePerDimension = newDistancePerDimension;
			}
			this.initIndex(
					model,
					distancePerDimension);

		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(
					e);
		}

	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Class<?> scope,
			final Configuration configuration ) {
		final ParameterEnum[] params = new ParameterEnum[] {
			CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
			PartitionParameters.Partition.DISTANCE_THRESHOLDS,
			Partition.PARTITION_PRECISION
		};
		runTimeProperties.setConfig(
				params,
				configuration,
				scope);
	}

	protected void initIndex(
			final CommonIndexModel indexModel,
			final double[] distancePerDimensionForIndex ) {

		// truncating to lower precision
		final NumericDimensionField<?>[] dimensions = indexModel.getDimensions();

		int totalRequestedPrecision = 0;
		final int[] dimensionPrecision = new int[indexModel.getDimensions().length];
		for (int i = 0; i < dimensionPrecision.length; i++) {
			final double distance = distancePerDimensionForIndex[i] * 2.0; // total
			// width...(radius)
			// adjust by precision factory (0 to 1.0)
			dimensionPrecision[i] = (int) (precisionFactor * Math.abs((int) (Math.log(dimensions[i].getRange()
					/ distance) / Math.log(2))));

			totalRequestedPrecision += dimensionPrecision[i];
		}
		if (totalRequestedPrecision > 63) {
			final double rescale = 63.0 / totalRequestedPrecision;
			for (int i = 0; i < dimensionPrecision.length; i++) {
				dimensionPrecision[i] = (int) (rescale * dimensionPrecision[i]);
			}
		}

		final TieredSFCIndexStrategy indexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
				indexModel.getDimensions(),
				dimensionPrecision,
				SFCType.HILBERT);

		// Not relevant since this is a single tier strategy.
		// For now, just setting to a non-zero reasonable value
		indexStrategy.setMaxEstimatedDuplicateIdsPerDimension(2);

		index = new PrimaryIndex(
				indexStrategy,
				indexModel);

	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		return Arrays.asList(new ParameterEnum<?>[] {
			CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
			PartitionParameters.Partition.DISTANCE_THRESHOLDS,
			Partition.PARTITION_PRECISION
		});

	}

	private void writeObject(
			ObjectOutputStream stream )
			throws IOException {
		final byte[] indexData = PersistenceUtils.toBinary(this.index);
		stream.writeInt(indexData.length);
		stream.write(indexData);
		stream.writeDouble(precisionFactor);
		stream.writeInt(distancePerDimension.length);
		for (double v : distancePerDimension)
			stream.writeDouble(v);
	}

	private void readObject(
			java.io.ObjectInputStream stream )
			throws IOException,
			ClassNotFoundException {
		final byte[] indexData = new byte[stream.readInt()];
		stream.readFully(indexData);
		index = (PrimaryIndex) PersistenceUtils.fromBinary(indexData);
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
		result = prime * result + Arrays.hashCode(distancePerDimension);
		result = prime * result + ((index == null) ? 0 : index.hashCode());
		long temp;
		temp = Double.doubleToLongBits(precisionFactor);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		AbstractPartitioner other = (AbstractPartitioner) obj;
		if (!Arrays.equals(
				distancePerDimension,
				other.distancePerDimension)) return false;
		if (index == null) {
			if (other.index != null) return false;
		}
		else if (!index.equals(other.index)) return false;
		if (Double.doubleToLongBits(precisionFactor) != Double.doubleToLongBits(other.precisionFactor)) return false;
		return true;
	}

}
