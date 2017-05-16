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
package mil.nga.giat.geowave.analytic.param;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

public class OutputStoreParameterHelper implements
		ParameterHelper<PersistableStore>
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	final static Logger LOGGER = LoggerFactory.getLogger(OutputStoreParameterHelper.class);

	@Override
	public Class<PersistableStore> getBaseClass() {
		return PersistableStore.class;
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final PersistableStore value ) {
		final DataStorePluginOptions options = value.getDataStoreOptions();
		GeoWaveOutputFormat.setStoreOptions(
				config,
				options);

	}

	@Override
	public PersistableStore getValue(
			final JobContext context,
			final Class<?> scope,
			final PersistableStore defaultValue ) {
		final DataStorePluginOptions pluginOptions = GeoWaveOutputFormat.getStoreOptions(context);
		if (pluginOptions != null) {
			return new PersistableStore(
					pluginOptions);
		}
		else {
			return defaultValue;
		}
	}

	@Override
	public PersistableStore getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return (PersistableStore) propertyManagement.getProperty(StoreParameters.StoreParam.OUTPUT_STORE);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize data store",
					e);
			return null;
		}
	}

	@Override
	public void setValue(
			final PropertyManagement propertyManagement,
			final PersistableStore value ) {
		propertyManagement.store(
				StoreParameters.StoreParam.OUTPUT_STORE,
				value);
	}
}
