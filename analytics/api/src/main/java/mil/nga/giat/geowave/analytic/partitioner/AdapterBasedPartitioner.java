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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SerializableAdapterStore;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.partitioner.AdapterBasedPartitioner.AdapterDataEntry;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

/**
 * This class uses the {@link DataAdapter} to decode the dimension fields to be
 * indexed. Although seemingly more flexible than the
 * {@link OrthodromicDistancePartitioner}, handling different types of data
 * entries, the assumption is that each object decode by the adapter provides
 * the fields required according to the supplied model.
 * 
 * The user provides the distances per dimension. It us up to the user to
 * convert geographic distance into distance in degrees per longitude and
 * latitude.
 * 
 * This class depends on an AdapterStore. Since an AdapterStore is not
 * Serializable, the dependency is transient requiring initialization after
 * serialization
 * {@link AdapterBasedPartitioner#initialize(ConfigurationWrapper)
 * 
 * 
 */
public class AdapterBasedPartitioner extends
		AbstractPartitioner<AdapterDataEntry> implements
		Partitioner<AdapterDataEntry>,
		Serializable
{

	final static Logger LOGGER = LoggerFactory.getLogger(AdapterBasedPartitioner.class);

	private static final long serialVersionUID = 5951564193108204266L;

	private NumericData[] fullRangesPerDimension;
	private boolean[] wrapsAroundBoundary;
	private SerializableAdapterStore adapterStore;

	public AdapterBasedPartitioner() {

	}

	public AdapterBasedPartitioner(
			final CommonIndexModel indexModel,
			final double[] distancesPerDimension,
			final AdapterStore adapterStore ) {
		super(
				indexModel,
				distancesPerDimension);
		this.adapterStore = new SerializableAdapterStore(
				adapterStore);
		init();
	}

	public static class AdapterDataEntry
	{
		ByteArrayId adapterId;
		Object data;

		public AdapterDataEntry(
				final ByteArrayId adapterId,
				final Object data ) {
			super();
			this.adapterId = adapterId;
			this.data = data;
		}
	}

	@Override
	protected NumericDataHolder getNumericData(
			final AdapterDataEntry entry ) {
		final NumericDataHolder numericDataHolder = new NumericDataHolder();

		@SuppressWarnings("unchecked")
		final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterStore.getAdapter(entry.adapterId);
		if (adapter == null) {
			LOGGER.error(
					"Unable to find an adapter for id {}",
					entry.adapterId.toString());
			return null;
		}
		final AdapterPersistenceEncoding encoding = adapter.encode(
				entry.data,
				getIndex().getIndexModel());
		final double[] thetas = getDistancePerDimension();
		final MultiDimensionalNumericData primaryData = encoding.getNumericData(getIndex()
				.getIndexModel()
				.getDimensions());
		numericDataHolder.primary = primaryData;
		numericDataHolder.expansion = querySet(
				primaryData,
				thetas);
		return numericDataHolder;
	}

	protected void init() {
		final NumericDimensionDefinition[] definitions = getIndex().getIndexStrategy().getOrderedDimensionDefinitions();
		fullRangesPerDimension = new NumericData[definitions.length];
		wrapsAroundBoundary = new boolean[definitions.length];
		for (int i = 0; i < definitions.length; i++) {
			fullRangesPerDimension[i] = definitions[i].getFullRange();
			wrapsAroundBoundary[i] = getIndex().getIndexModel().getDimensions()[i].getBaseDefinition() instanceof LongitudeDefinition;
		}

	}

	@Override
	public void initialize(
			final JobContext context,
			final Class<?> scope )
			throws IOException {
		super.initialize(
				context,
				scope);
		adapterStore = new SerializableAdapterStore(
				((PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
						context,
						scope,
						null)).getDataStoreOptions().createAdapterStore());

		init();
	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Class<?> scope,
			final Configuration configuration ) {
		super.setup(
				runTimeProperties,
				scope,
				configuration);
		final ParameterEnum[] params = new ParameterEnum[] {
			StoreParameters.StoreParam.INPUT_STORE
		};
		runTimeProperties.setConfig(
				params,
				configuration,
				scope);
	}

	protected MultiDimensionalNumericData[] querySet(
			final MultiDimensionalNumericData dimensionsData,
			final double[] distances ) {

		final List<NumericRange[]> resultList = new ArrayList<NumericRange[]>();
		final NumericRange[] currentData = new NumericRange[dimensionsData.getDimensionCount()];
		addToList(
				resultList,
				currentData,
				distances,
				dimensionsData,
				0);
		final MultiDimensionalNumericData[] finalSet = new MultiDimensionalNumericData[resultList.size()];
		int i = 0;
		for (final NumericRange[] rangeData : resultList) {
			finalSet[i++] = new BasicNumericDataset(
					rangeData);
		}
		return finalSet;
	}

	private void addToList(
			final List<NumericRange[]> resultList,
			final NumericRange[] currentData,
			final double[] distances,
			final MultiDimensionalNumericData dimensionsData,
			final int d ) {
		if (d == currentData.length) {
			resultList.add(Arrays.copyOf(
					currentData,
					currentData.length));
			return;
		}

		final NumericData dimensionData = dimensionsData.getDataPerDimension()[d];
		final double lowerBound = dimensionData.getMin() - distances[d];
		final double upperBound = dimensionData.getMax() + distances[d];

		final double mindiff = lowerBound - fullRangesPerDimension[d].getMin();
		final double maxdiff = upperBound - fullRangesPerDimension[d].getMax();
		if (wrapsAroundBoundary[d] && (mindiff < 0)) {
			currentData[d] = new NumericRange(
					fullRangesPerDimension[d].getMax() + mindiff,
					fullRangesPerDimension[d].getMax());
			addToList(
					resultList,
					currentData,
					distances,
					dimensionsData,
					d + 1);
			currentData[d] = new NumericRange(
					fullRangesPerDimension[d].getMin(),
					upperBound);
			addToList(
					resultList,
					currentData,
					distances,
					dimensionsData,
					d + 1);
		}
		else if (wrapsAroundBoundary[d] && (maxdiff > 0)) {
			currentData[d] = new NumericRange(
					lowerBound,
					fullRangesPerDimension[d].getMax());
			addToList(
					resultList,
					currentData,
					distances,
					dimensionsData,
					d + 1);
			currentData[d] = new NumericRange(
					fullRangesPerDimension[d].getMin(),
					fullRangesPerDimension[d].getMin() + maxdiff);
			addToList(
					resultList,
					currentData,
					distances,
					dimensionsData,
					d + 1);
		}
		else {
			currentData[d] = new NumericRange(
					lowerBound,
					upperBound);
			addToList(
					resultList,
					currentData,
					distances,
					dimensionsData,
					d + 1);
		}

	}

}
