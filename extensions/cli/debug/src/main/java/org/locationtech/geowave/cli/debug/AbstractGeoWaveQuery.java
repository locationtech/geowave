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
package org.locationtech.geowave.cli.debug;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.time.StopWatch;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.converters.GeoWaveBaseConverter;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

abstract public class AbstractGeoWaveQuery extends
		DefaultOperation implements
		Command
{
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWaveQuery.class);

	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = "--indexId", description = "The name of the index (optional)", converter = StringToByteArrayConverter.class)
	private ByteArrayId indexId;

	@Parameter(names = "--adapterId", description = "Optional ability to provide an adapter ID", converter = StringToByteArrayConverter.class)
	private ByteArrayId adapterId;

	@Parameter(names = "--debug", description = "Print out additional info for debug purposes")
	private boolean debug = false;

	@Override
	public void execute(
			OperationParams params )
			throws ParseException {
		final StopWatch stopWatch = new StopWatch();

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <storename>");
		}

		String storeName = parameters.get(0);

		// Attempt to load store.
		StoreLoader storeOptions = new StoreLoader(
				storeName);
		if (!storeOptions.loadFromConfig(getGeoWaveConfigFile(params))) {
			throw new ParameterException(
					"Cannot find store name: " + storeOptions.getStoreName());
		}

		DataStore dataStore;
		PersistentAdapterStore adapterStore;
		try {
			dataStore = storeOptions.createDataStore();
			adapterStore = storeOptions.createAdapterStore();

			final GeotoolsFeatureDataAdapter adapter;
			if (adapterId != null) {
				adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(
						storeOptions.createInternalAdapterStore().getInternalAdapterId(
								adapterId)).getAdapter();
			}
			else {
				final CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters();
				adapter = (GeotoolsFeatureDataAdapter) it.next().getAdapter();
				it.close();
			}
			if (debug && (adapter != null)) {
				System.out.println(adapter);
			}
			stopWatch.start();
			final long results = runQuery(
					adapter,
					adapterId,
					indexId,
					dataStore,
					debug);
			stopWatch.stop();
			System.out.println("Got " + results + " results in " + stopWatch.toString());
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to read adapter",
					e);
		}
	}

	abstract protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			DataStore dataStore,
			boolean debug );

	public static class StringToByteArrayConverter extends
			GeoWaveBaseConverter<ByteArrayId>
	{
		public StringToByteArrayConverter(
				String optionName ) {
			super(
					optionName);
		}

		@Override
		public ByteArrayId convert(
				String value ) {
			return new ByteArrayId(
					value);
		}
	}

}
