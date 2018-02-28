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
package mil.nga.giat.geowave.format.theia;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaDownloadCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaRasterIngestCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaSection;
import mil.nga.giat.geowave.format.theia.RasterIngestRunner;

@GeowaveOperation(name = "ingestraster", parentOperation = TheiaSection.class)
@Parameters(commandDescription = "Ingest routine for locally downloading Theia imagery and ingesting it into GeoWave.")
public class TheiaIngestRasterCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<storename> <comma delimited index/group list>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	protected TheiaBasicCommandLineOptions analyzeOptions = new TheiaBasicCommandLineOptions();

	@ParametersDelegate
	protected TheiaDownloadCommandLineOptions downloadOptions = new TheiaDownloadCommandLineOptions();

	@ParametersDelegate
	protected TheiaRasterIngestCommandLineOptions ingestOptions = new TheiaRasterIngestCommandLineOptions();

	public TheiaIngestRasterCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		JAIExt.initJAIEXT();

		final RasterIngestRunner runner = new RasterIngestRunner(
				analyzeOptions,
				downloadOptions,
				ingestOptions,
				parameters);
		runner.runInternal(params);
	}
}
