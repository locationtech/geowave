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
package org.locationtech.geowave.core.store.operations.remote;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.RemoteSection;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexLoader;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "mergedata", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Merge all rows for a given adapter and index")
public class MergeDataCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<storename> <indexname>")
	private List<String> parameters = new ArrayList<String>();

	private DataStorePluginOptions inputStoreOptions = null;

	private List<IndexPluginOptions> inputIndexOptions = null;

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <storename> <indexname>");
		}

		final String inputStoreName = parameters.get(0);
		final String indexList = parameters.get(1);

		final StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		// Load the Indexes
		final IndexLoader indexLoader = new IndexLoader(
				indexList);
		if (!indexLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
			throw new ParameterException(
					"Cannot find index(s) by name: " + indexList);
		}

		inputIndexOptions = indexLoader.getLoadedIndexes();
		final PersistentAdapterStore adapterStore = inputStoreOptions.createAdapterStore();
		final InternalAdapterStore internalAdapterStore = inputStoreOptions.createInternalAdapterStore();
		final AdapterIndexMappingStore adapterIndexMappingStore = inputStoreOptions.createAdapterIndexMappingStore();
		final DataStoreOperations operations = inputStoreOptions.createDataStoreOperations();

		for (final IndexPluginOptions i : inputIndexOptions) {
			final Index index = i.createIndex();
			if (!operations.mergeData(
					index,
					adapterStore,
					internalAdapterStore,
					adapterIndexMappingStore)) {
				JCommander.getConsole().println(
						"Unable to merge data within index '" + index.getName() + "'");
			}
			else {
				JCommander.getConsole().println(
						"Data successfully merged within index '" + index.getName() + "'");
			}
		}

	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName,
			final String adapterId ) {
		parameters = Arrays.asList(
				storeName,
				adapterId);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}
}
