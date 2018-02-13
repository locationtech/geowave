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
package mil.nga.giat.geowave.test.services;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStoreImpl;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;

public class ServicesTestUtils
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicesTestUtils.class);

	protected static void writeConfigFile(
			final File configFile ) {
		try {
			final PrintWriter writer = new PrintWriter(
					configFile,
					StringUtils.GEOWAVE_CHAR_SET.toString());
			// just enable all store types through services regardless of which
			// ones are required
			for (final GeoWaveStoreType type : GeoWaveStoreType.values()) {
				final Properties storeProps = new Properties();
				final DataStorePluginOptions storeOptions = type.getTestEnvironment().getDataStoreOptions(
						new GeoWaveTestStoreImpl(
								TestUtils.TEST_NAMESPACE,
								new GeoWaveStoreType[] {
									type
								},
								new String[] {},
								GeoWaveTestStore.class));
				storeOptions.save(
						storeProps,
						DataStorePluginOptions.getStoreNamespace(type.name()));
				try {
					storeProps.store(
							writer,
							"Data Store Options for '" + type.name() + "'");
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to write store options for '" + type.name() + "'",
							e);
				}
			}
			final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();

			writer.println("geoserver.url=" + ServicesTestEnvironment.GEOSERVER_BASE_URL);
			writer.println("geoserver.username=" + ServicesTestEnvironment.GEOSERVER_USER);
			writer.println("geoserver.password=" + ServicesTestEnvironment.GEOSERVER_PASS);
			writer.println("geoserver.workspace=" + ServicesTestEnvironment.TEST_WORKSPACE);
			writer.println("hdfs=" + env.getHdfs());
			writer.println("hdfsBase=" + env.getHdfsBaseDirectory());
			writer.println("jobTracker=" + env.getJobtracker());
			writer.close();
		}
		catch (final FileNotFoundException e) {
			LOGGER.error(
					"Unable to find config file",
					e);
		}
		catch (final UnsupportedEncodingException e) {
			LOGGER.error(
					"Unable to write config file in UTF-8",
					e);
		}
	}
}
