/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.theia;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import com.beust.jcommander.ParameterException;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.VectorIngestRunner;
import mil.nga.giat.geowave.core.store.CloseableIterator;

public class VectorIngestRunnerTest
{
	@BeforeClass
	public static void setup()
			throws IOException {
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
	}

	@Test
	public void testIngest()
			throws Exception {
		JAIExt.initJAIEXT();

		TheiaBasicCommandLineOptions analyzeOptions = new TheiaBasicCommandLineOptions();
		analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);
		analyzeOptions.setLocation("T30TXN");
		analyzeOptions.setStartDate(DateUtilities.parseISO("2018-01-01T00:00:00Z"));
		analyzeOptions.setEndDate(DateUtilities.parseISO("2018-01-03T00:00:00Z"));
		analyzeOptions.setCqlFilter("BBOX(shape,-1.8274,42.3253,-1.6256,42.4735) AND (band='B4' OR band='B8')");

		VectorIngestRunner runner = new VectorIngestRunner(
				analyzeOptions,
				Arrays.asList(
						"memorystore",
						"spatialindex,spatempindex"));

		ManualOperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				new File(
						VectorIngestRunnerTest.class.getClassLoader().getResource(
								"geowave-config.properties").toURI()));

		runner.runInternal(params);

		try (CloseableIterator<Object> results = getStore(
				params).query(
				new QueryOptions(),
				new EverythingQuery())) {
			assertTrue(
					"Store is empty when it should have at least one result",
					results.hasNext());
		}

		// Not sure what assertions can be made about the indexes.
	}

	private DataStore getStore(
			OperationParams params ) {
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		StoreLoader inputStoreLoader = new StoreLoader(
				"memorystore");
		if (!inputStoreLoader.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}

		DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();
		return storeOptions.createDataStore();
	}
}
