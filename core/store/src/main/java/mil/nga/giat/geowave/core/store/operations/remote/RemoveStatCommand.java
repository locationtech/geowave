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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StatsCommandLineOptions;

@GeowaveOperation(name = "rmstat", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Remove a statistic from the remote store. You will be prompted with are you sure")
public class RemoveStatCommand extends
		AbstractStatsCommand<Void>
{

	@Parameter(description = "<store name> <adapterId> <statId>")
	private List<String> parameters = new ArrayList<String>();

	private String statId = null;

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

		// Remove the stat
		final DataStatisticsStore statStore = storeOptions.createDataStatisticsStore();
		final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

		if (!statStore.removeStatistics(
				adapter.getAdapterId(),
				new ByteArrayId(
						statId),
				authorizations)) {
			throw new RuntimeException(
					"Unable to remove statistic: " + statId);
		}

		return true;
	}

	@Override
	public Void computeResults(
			final OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <store name> <adapterId> <statId>");
		}

		statId = parameters.get(2);

		super.run(
				params,
				parameters);
		return null;
	}

}
