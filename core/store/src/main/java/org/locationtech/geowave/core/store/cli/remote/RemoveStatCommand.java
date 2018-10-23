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
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StatsCommandLineOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rmstat", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Remove a statistic from the remote store. You will be prompted with are you sure")
public class RemoveStatCommand extends
		AbstractStatsCommand<Void>
{

	@Parameter(description = "<store name> <datatype name> <stat type>")
	private List<String> parameters = new ArrayList<>();

	@Parameter(names = {
		"--fieldName"
	}, description = "If the statistic is maintained per field, optionally provide a field name")
	private String fieldName = "";

	private String statType = null;

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}

	@Override
	protected boolean performStatsCommand(
			final DataStorePluginOptions storeOptions,
			final InternalDataAdapter<?> adapter,
			final StatsCommandLineOptions statsOptions )
			throws IOException {

		// Remove the stat
		final DataStatisticsStore statStore = storeOptions.createDataStatisticsStore();
		final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

		if (!statStore.removeStatistics(
				adapter.getAdapterId(),
				fieldName,
				new BaseStatisticsType<>(
						statType),
				authorizations)) {
			throw new RuntimeException(
					"Unable to remove statistic: " + statType);
		}

		return true;
	}

	@Override
	public Void computeResults(
			final OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <store name> <datatype name> <stat type>");
		}

		statType = parameters.get(2);

		super.run(
				params,
				parameters);
		return null;
	}

}
