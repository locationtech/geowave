/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.param.ExtractParameters;
import org.locationtech.geowave.analytic.param.FormatConfiguration;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;

public class GeoWaveInputFormatConfiguration implements
		FormatConfiguration
{

	protected boolean isDataWritable = false;
	protected List<DataTypeAdapter<?>> adapters = new ArrayList<>();
	protected List<Index> indices = new ArrayList<>();

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

		final Query<?> query = runTimeProperties.getPropertyAsQuery(ExtractParameters.Extract.QUERY);

		if (query != null) {
			if (query.getQueryConstraints() != null) {
				GeoWaveInputFormat.setQueryConstraints(
						configuration,
						(QueryConstraints) query.getQueryConstraints());
			}

			if (query.getCommonQueryOptions() != null) {
				GeoWaveInputFormat.setCommonQueryOptions(
						configuration,
						query.getCommonQueryOptions());
			}

			if (query.getDataTypeQueryOptions() != null) {
				GeoWaveInputFormat.setDataTypeQueryOptions(
						configuration,
						query.getDataTypeQueryOptions(),
						dataStoreOptions.createAdapterStore(),
						dataStoreOptions.createInternalAdapterStore());
			}

			if (query.getIndexQueryOptions() != null) {
				GeoWaveInputFormat.setIndexQueryOptions(
						configuration,
						query.getIndexQueryOptions(),
						dataStoreOptions.createIndexStore());
			}
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
			final DataTypeAdapter<?> adapter ) {
		adapters.add(adapter);
	}

	public void addIndex(
			final Index index ) {
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
			ExtractParameters.Extract.QUERY,
			ExtractParameters.Extract.MAX_INPUT_SPLIT,
			ExtractParameters.Extract.MIN_INPUT_SPLIT,
			StoreParam.INPUT_STORE
		});
	}
}
