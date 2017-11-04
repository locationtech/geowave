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

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class RemoteOperationsProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		RemoteSection.class,
		CalculateStatCommand.class,
		ClearCommand.class,
		ListAdapterCommand.class,
		ListIndexCommand.class,
		ListStatsCommand.class,
		VersionCommand.class,
		MergeDataCommand.class,
		RecalculateStatsCommand.class,
		RemoveAdapterCommand.class,
		RemoveStatCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
