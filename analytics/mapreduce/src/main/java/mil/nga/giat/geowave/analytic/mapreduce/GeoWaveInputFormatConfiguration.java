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
package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

public class GeoWaveInputFormatConfiguration implements
		FormatConfiguration
{

	protected boolean isDataWritable = false;
	protected List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	protected List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>();

	public GeoWaveInputFormatConfiguration() {

	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration )
			throws Exception {
		final DataStorePluginOptions dataStoreOptions = ((PersistableStore) runTimeProperties
				.getProperty(StoreParam.INPUT_STORE)).getDataStoreOptions();
		GeoWaveInputFormat.setStoreOptions(
				configuration,
				dataStoreOptions);

		final DistributableQuery query = runTimeProperties.getPropertyAsQuery(ExtractParameters.Extract.QUERY);

		if (query != null) {
			GeoWaveInputFormat.setQuery(
					configuration,
					query);
		}

		final QueryOptions queryoptions = runTimeProperties
				.getPropertyAsQueryOptions(ExtractParameters.Extract.QUERY_OPTIONS);

		if (queryoptions != null) {
			GeoWaveInputFormat.setQueryOptions(
					configuration,
					queryoptions);
		}

		final int minInputSplits = runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MIN_INPUT_SPLIT,
				-1);
		if (minInputSplits > 0) {
			GeoWaveInputFormat.setMinimumSplitCount(
					configuration,
					minInputSplits);
		}
		final int maxInputSplits = runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MAX_INPUT_SPLIT,
				-1);
		if (maxInputSplits > 0) {
			GeoWaveInputFormat.setMaximumSplitCount(
					configuration,
					maxInputSplits);
		}

		GeoWaveInputFormat.setIsOutputWritable(
				configuration,
				isDataWritable);
	}

	public void addDataAdapter(
			final DataAdapter<?> adapter ) {
		adapters.add(adapter);
	}

	public void addIndex(
			final PrimaryIndex index ) {
		indices.add(index);
	}

	@Override
	public Class<?> getFormatClass() {
		return GeoWaveInputFormat.class;
	}

	@Override
	public boolean isDataWritable() {
		return isDataWritable;
	}

	@Override
	public void setDataIsWritable(
			final boolean isWritable ) {
		isDataWritable = isWritable;

	}

	@Override
	public List<ParameterEnum<?>> getParameters() {
		return Arrays.asList(new ParameterEnum<?>[] {
			ExtractParameters.Extract.QUERY_OPTIONS,
			ExtractParameters.Extract.QUERY,
			ExtractParameters.Extract.MAX_INPUT_SPLIT,
			ExtractParameters.Extract.MIN_INPUT_SPLIT,
			StoreParam.INPUT_STORE
		});
	}
}
