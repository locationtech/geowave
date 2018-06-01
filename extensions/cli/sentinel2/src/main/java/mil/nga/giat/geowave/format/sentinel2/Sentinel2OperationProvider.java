/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.sentinel2;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2AnalyzeCommand;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2DownloadCommand;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2IngestCommand;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2IngestRasterCommand;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2IngestVectorCommand;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2Section;

public class Sentinel2OperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		Sentinel2Section.class,
		Sentinel2AnalyzeCommand.class,
		Sentinel2DownloadCommand.class,
		Sentinel2IngestCommand.class,
		Sentinel2IngestRasterCommand.class,
		Sentinel2IngestVectorCommand.class,
		Sentinel2ImageryProvidersCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
