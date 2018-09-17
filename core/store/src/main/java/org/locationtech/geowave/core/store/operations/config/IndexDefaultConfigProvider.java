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
package org.locationtech.geowave.core.store.operations.config;

import java.util.Properties;

import org.locationtech.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class IndexDefaultConfigProvider implements
		DefaultConfigProviderSpi
{
	private Properties configProperties = new Properties();

	/**
	 * Create the properties for the config-properties file
	 */
	private void setProperties() {
		// Spatial Index
		configProperties.setProperty(
				"index.default-spatial.opts.numPartitions",
				"8");
		configProperties.setProperty(
				"index.default-spatial.opts.partitionStrategy",
				"HASH");
		configProperties.setProperty(
				"index.default-spatial.opts.storeTime",
				"false");
		configProperties.setProperty(
				"index.default-spatial.type",
				"spatial");
		// Spatial_Temporal Index
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.bias",
				"BALANCED");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.maxDuplicates",
				"-1");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.numPartitions",
				"8");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.partitionStrategy",
				"HASH");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.period",
				"YEAR");
		configProperties.setProperty(
				"index.default-spatial-temporal.type",
				"spatial_temporal");
	}

	@Override
	public Properties getDefaultConfig() {
		setProperties();
		return configProperties;
	}

}
