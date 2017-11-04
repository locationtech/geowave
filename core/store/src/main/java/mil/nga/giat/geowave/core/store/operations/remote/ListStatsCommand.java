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
package mil.nga.giat.geowave.core.store.operations.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StatsCommandLineOptions;
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
		"--adapterId"
	}, description = "Optionally list a single adapter's stats")
	private String adapterId = "";

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	private String retValue = "";

	private ServiceStatus status = ServiceStatus.OK;

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}

	@Override
	protected boolean performStatsCommand(
			final DataStorePluginOptions storeOptions,
			final DataAdapter<?> adapter,
			final StatsCommandLineOptions statsOptions )
			throws IOException {

		if (adapter == null) {
			throw new IOException(
					"Provided adapter is null");
		}

		final DataStatisticsStore statsStore = storeOptions.createDataStatisticsStore();
		final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

		final StringBuilder builder = new StringBuilder();

		try (CloseableIterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics(authorizations)) {
			if (statsOptions.getJsonFormatFlag()) {
				final JSONArray resultsArray = new JSONArray();
				final JSONObject outputObject = new JSONObject();

				try {
					// Output as JSON formatted strings
					outputObject.put(
							"adapter",
							adapter.getAdapterId().getString());
					while (statsIt.hasNext()) {
						final DataStatistics<?> stats = statsIt.next();
						if (!stats.getDataAdapterId().equals(
								adapter.getAdapterId())) {
							continue;
						}
						resultsArray.add(stats.toJSONObject());
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
					final DataStatistics<?> stats = statsIt.next();
					if (!stats.getDataAdapterId().equals(
							adapter.getAdapterId())) {
						continue;
					}
					builder.append("[");
					builder.append(String.format(
							"%1$-20s",
							stats.getStatisticsId().getString()));
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
	public Pair<ServiceStatus, String> executeService(
			OperationParams params )
			throws Exception {
		String ret = computeResults(params);
		return ImmutablePair.of(
				status,
				ret);
	}

	@Override
	public String computeResults(
			final OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Requires arguments: <store name>");
		}
		if ((adapterId != null) && !adapterId.trim().isEmpty()) {
			parameters.add(adapterId);
		}
		super.run(
				params,
				parameters);
		if (!retValue.equals("")) {
			setStatus(ServiceStatus.OK);
			return retValue;
		}
		else {
			setStatus(ServiceStatus.NOT_FOUND);
			return "No Data Found";
		}
	}

	public ServiceStatus getStatus() {
		return status;
	}

	public void setStatus(
			ServiceStatus status ) {
		this.status = status;
	}
}
