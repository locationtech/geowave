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
package org.locationtech.geowave.adapter.vector.delete;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.time.StopWatch;
import org.locationtech.geowave.adapter.vector.cli.VectorSection;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "cqldelete", parentOperation = VectorSection.class)
@Parameters(commandDescription = "Delete data that matches a CQL filter")
public class CQLDelete extends
		DefaultOperation implements
		Command
{
	private static Logger LOGGER = LoggerFactory.getLogger(CQLDelete.class);

	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<>();

	@Parameter(names = "--cql", required = true, description = "CQL Filter for delete")
	private String cqlStr;

	@Parameter(names = "--indexName", required = false, description = "The name of the index (optional)", converter = StringToByteArrayConverter.class)
	private String indexName;

	@Parameter(names = "--typeName", required = false, description = "Optional ability to provide a type name for the data adapter", converter = StringToByteArrayConverter.class)
	private String typeName;

	@Parameter(names = "--debug", required = false, description = "Print out additional info for debug purposes")
	private boolean debug = false;

	@Override
	public void execute(
			final OperationParams params )
			throws ParseException {
		if (debug) {
			org.apache.log4j.Logger.getRootLogger().setLevel(
					org.apache.log4j.Level.DEBUG);
		}

		final StopWatch stopWatch = new StopWatch();

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <storename>");
		}

		final String storeName = parameters.get(0);

		// Config file
		final File configFile = getGeoWaveConfigFile(params);

		// Attempt to load store.
		final StoreLoader storeOptions = new StoreLoader(
				storeName);
		if (!storeOptions.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + storeOptions.getStoreName());
		}

		final DataStore dataStore = storeOptions.createDataStore();
		final PersistentAdapterStore adapterStore = storeOptions.createAdapterStore();
		final InternalAdapterStore internalAdapterStore = storeOptions.createInternalAdapterStore();

		final GeotoolsFeatureDataAdapter adapter;
		if (typeName != null) {
			adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(
					internalAdapterStore.getAdapterId(typeName)).getAdapter();
		}
		else {
			final CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters();
			adapter = (GeotoolsFeatureDataAdapter) it.next().getAdapter();
			it.close();
		}

		if (debug && (adapter != null)) {
			LOGGER.debug(adapter.toString());
		}

		stopWatch.start();
		final long results = delete(
				adapter,
				typeName,
				indexName,
				dataStore,
				debug);
		stopWatch.stop();

		if (debug) {
			LOGGER.debug(results + " results remaining after delete; time = " + stopWatch.toString());
		}
	}

	protected long delete(
			final GeotoolsFeatureDataAdapter adapter,
			final String typeName,
			final String indexName,
			final DataStore dataStore,
			final boolean debug ) {
		long missed = 0;

		final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
		final Query<SimpleFeature> query = bldr.addTypeName(
				typeName).indexName(
				indexName).constraints(
				bldr.constraintsFactory().cqlConstraints(
						cqlStr)).build();
		final boolean success = dataStore.delete(query);

		if (debug) {
			LOGGER.debug("CQL Delete " + (success ? "Success" : "Failure"));
		}

		// Verify delete by running the CQL query
		if (debug) {
			try (final CloseableIterator<SimpleFeature> it = dataStore.query(query)) {

				while (it.hasNext()) {
					it.next();
					missed++;
				}
			}
		}

		return missed;
	}

	public static class StringToByteArrayConverter implements
			IStringConverter<ByteArray>
	{
		@Override
		public ByteArray convert(
				final String value ) {
			return new ByteArray(
					value);
		}
	}
}
