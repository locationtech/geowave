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
package mil.nga.giat.geowave.core.store.operations.config;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

@GeowaveOperation(name = "rmstore", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Remove store from Geowave configuration")
public class RemoveStoreCommand extends
		AbstractRemoveCommand
{

	@Override
	public String computeResults(

			final OperationParams params ) {

		// Search for properties relevant to the given name
		pattern = DataStorePluginOptions.getStoreNamespace(getEntryName());
		return super.computeResults(
				params,
				pattern);

	}

	@Override
	public Pair<ServiceStatus, String> executeService(
			OperationParams params )
			throws Exception {
		String ret = computeResults(params);
		return ImmutablePair.of(
				super.getStatus(),
				ret);
	}

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}
}
