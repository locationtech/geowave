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
package org.locationtech.geowave.core.store.cli.remote;

import java.io.IOException;
import java.util.List;

import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StatsCommandLineOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

/**
 * Common methods for dumping, manipulating and calculating stats.
 */
public abstract class AbstractStatsCommand<T> extends
		ServiceEnabledCommand<T>
{

	/**
	 * Return "200 OK" for all stats commands.
	 */
	@Override
	public Boolean successStatusIs200() {
		return true;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(RecalculateStatsCommand.class);

	@ParametersDelegate
	private StatsCommandLineOptions statsOptions = new StatsCommandLineOptions();

	public void run(
			final OperationParams params,
			final List<String> parameters ) {

		final String storeName = parameters.get(0);
		String typeName = null;
		if (parameters.size() > 1) {
			typeName = parameters.get(1);
		}

		// Attempt to load input store if not already provided (test purposes).

		final StoreLoader inputStoreLoader = new StoreLoader(
				storeName);
		if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		try {
			// Various stores needed
			final PersistentAdapterStore adapterStore = inputStoreOptions.createAdapterStore();

			if (typeName != null) {
				final InternalAdapterStore internalAdapterStore = inputStoreOptions.createInternalAdapterStore();
				final Short adapterId = internalAdapterStore.getAdapterId(typeName);
				InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
				if (adapter != null) {
					performStatsCommand(
							inputStoreOptions,
							adapter,
							statsOptions);
				}
				else {
					// If this adapter is not known, provide list of available
					// adapters
					LOGGER.error("Unknown adapter " + adapterId);
					final StringBuffer buffer = new StringBuffer();
					for (String t : internalAdapterStore.getTypeNames()) {
						buffer.append(
								t).append(
								' ');
					}
					LOGGER.info("Available data types: " + buffer.toString());
				}
			}
			else {
				// Repeat the Command for every adapter found
				try (CloseableIterator<InternalDataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
					while (adapterIt.hasNext()) {
						final InternalDataAdapter<?> adapter = adapterIt.next();
						if (!performStatsCommand(
								inputStoreOptions,
								adapter,
								statsOptions)) {
							LOGGER.info("Unable to calculate statistics for data type: " + adapter.getTypeName());
						}
					}
				}
			}
		}
		catch (final IOException e) {
			throw new RuntimeException(
					"Unable to parse stats tool arguments",
					e);
		}
	}

	/**
	 * Abstracted command method to be called when command selected
	 */

	abstract protected boolean performStatsCommand(
			final DataStorePluginOptions options,
			final InternalDataAdapter<?> adapter,
			final StatsCommandLineOptions statsOptions )
			throws IOException;

	/**
	 * Helper method to extract a list of authorizations from a string passed in
	 * from the command line
	 *
	 * @param auths
	 *            - String to be parsed
	 */
	protected static String[] getAuthorizations(
			final String auths ) {
		if ((auths == null) || (auths.length() == 0)) {
			return new String[0];
		}
		final String[] authsArray = auths.split(",");
		for (int i = 0; i < authsArray.length; i++) {
			authsArray[i] = authsArray[i].trim();
		}
		return authsArray;
	}

}
