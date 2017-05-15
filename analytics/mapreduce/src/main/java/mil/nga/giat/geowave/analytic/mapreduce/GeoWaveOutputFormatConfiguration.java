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

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

public class GeoWaveOutputFormatConfiguration implements
		FormatConfiguration
{
	/**
	 * Captures the state, but the output format is flexible enough to deal with
	 * both.
	 * 
	 */
	protected boolean isDataWritable = false;

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration )
			throws Exception {
		final DataStorePluginOptions dataStoreOptions = ((PersistableStore) runTimeProperties
				.getProperty(StoreParam.INPUT_STORE)).getDataStoreOptions();
		GeoWaveOutputFormat.setStoreOptions(
				configuration,
				dataStoreOptions);
	}

	@Override
	public Class<?> getFormatClass() {
		return GeoWaveOutputFormat.class;
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
	public Collection<ParameterEnum<?>> getParameters() {
		return Arrays.asList(new ParameterEnum<?>[] {
			StoreParam.INPUT_STORE
		});
	}
}
