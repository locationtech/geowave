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
import mil.nga.giat.geowave.format.sentinel2.RasterIngestRunner;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2BasicCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2DownloadCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2RasterIngestCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2Section;

@GeowaveOperation(name = "ingestraster", parentOperation = Sentinel2Section.class)
@Parameters(commandDescription = "Ingest routine for locally downloading Sentinel2 imagery and ingesting it into GeoWave.")
public class Sentinel2IngestRasterCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<storename> <comma delimited index/group list>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	protected Sentinel2BasicCommandLineOptions analyzeOptions = new Sentinel2BasicCommandLineOptions();

	@ParametersDelegate
	protected Sentinel2DownloadCommandLineOptions downloadOptions = new Sentinel2DownloadCommandLineOptions();

	@ParametersDelegate
	protected Sentinel2RasterIngestCommandLineOptions ingestOptions = new Sentinel2RasterIngestCommandLineOptions();

	public Sentinel2IngestRasterCommand() {}

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
