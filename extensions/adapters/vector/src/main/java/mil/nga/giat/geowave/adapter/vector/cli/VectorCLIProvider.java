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
package mil.nga.giat.geowave.adapter.vector.cli;

import mil.nga.giat.geowave.adapter.vector.delete.CQLDelete;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportCommand;
import mil.nga.giat.geowave.adapter.vector.export.VectorMRExportCommand;
import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class VectorCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		VectorSection.class,
		VectorLocalExportCommand.class,
		VectorMRExportCommand.class,
		CQLDelete.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
