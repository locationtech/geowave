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
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StatsCommandLineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "liststats", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Print statistics of an existing GeoWave dataset to standard output.  ")
public class ListStatsCommand extends
		AbstractStatsCommand<String> implements
		Command
{

	private static final Logger LOGGER = LoggerFactory.getLogger(ListStatsCommand.class);

	@Parameter(names = {
		"--typeName"
	}, description = "Optionally list a single data type's stats")
	private String typeName = "";

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	private String retValue = "";

	@Override
	public void execute(
			final OperationParams params )
			throws TargetNotFoundException {
		computeResults(params);
	}

	@Override
	protected boolean performStatsCommand(
			final DataStorePluginOptions storeOptions,
			final InternalDataAdapter<?> adapter,
			final StatsCommandLineOptions statsOptions )
			throws IOException {

		if (adapter == null) {
			throw new IOException(
					"Provided adapter is null");
		}

		final DataStatisticsStore statsStore = storeOptions.createDataStatisticsStore();
		final InternalAdapterStore internalAdapterStore = storeOptions.createInternalAdapterStore();
		final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

		final StringBuilder builder = new StringBuilder();

		try (CloseableIterator<InternalDataStatistics<?, ?, ?>> statsIt = statsStore
				.getAllDataStatistics(authorizations)) {
			if (statsOptions.getJsonFormatFlag()) {
				final JSONArray resultsArray = new JSONArray();
				final JSONObject outputObject = new JSONObject();

				try {
					// Output as JSON formatted strings
					outputObject.put(
							"dataType",
							adapter.getTypeName());
					while (statsIt.hasNext()) {
						final InternalDataStatistics<?, ?, ?> stats = statsIt.next();
						if (stats.getAdapterId() != adapter.getAdapterId()) {
							continue;
						}
						resultsArray.add(stats.toJSONObject(internalAdapterStore));
					}
					outputObject.put(
							"stats",
							resultsArray);
					builder.append(outputObject.toString());
				}
				catch (final JSONException ex) {
					LOGGER.error(
							"Unable to output statistic as JSON.  ",
							ex);
				}
			}
			// Output as strings
			else {
				while (statsIt.hasNext()) {
					final InternalDataStatistics<?, ?, ?> stats = statsIt.next();
					if (stats.getAdapterId() != adapter.getAdapterId()) {
						continue;
					}
					builder.append("[");
					builder.append(String.format(
							"%1$-20s",
							stats.getType().getString()));
					builder.append("] ");
					builder.append(stats.toString());
					builder.append("\n");
				}
			}
			retValue = builder.toString().trim();
			JCommander.getConsole().println(
					retValue);

		}

		return true;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName,
			final String adapterName ) {
		parameters = new ArrayList<String>();
		parameters.add(storeName);
		if (adapterName != null) {
			parameters.add(adapterName);
		}
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws TargetNotFoundException {
		// Ensure we have all the required arguments
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Requires arguments: <store name>");
		}
		if ((typeName != null) && !typeName.trim().isEmpty()) {
			parameters.add(typeName);
		}
		super.run(
				params,
				parameters);
		if (!retValue.equals("")) {
			return retValue;
		}
		else {
			return "No Data Found";
		}
	}
}
