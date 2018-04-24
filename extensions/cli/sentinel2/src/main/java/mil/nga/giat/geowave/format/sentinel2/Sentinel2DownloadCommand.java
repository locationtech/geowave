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

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.format.sentinel2.DownloadRunner;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2BasicCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2DownloadCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2Section;

@GeowaveOperation(name = "download", parentOperation = Sentinel2Section.class)
@Parameters(commandDescription = "Download Sentinel2 imagery to a local directory.")
public class Sentinel2DownloadCommand extends
		DefaultOperation implements
		Command
{
	@ParametersDelegate
	protected Sentinel2BasicCommandLineOptions analyzeOptions = new Sentinel2BasicCommandLineOptions();

	@ParametersDelegate
	protected Sentinel2DownloadCommandLineOptions downloadOptions = new Sentinel2DownloadCommandLineOptions();

	public Sentinel2DownloadCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		final DownloadRunner runner = new DownloadRunner(
				analyzeOptions,
				downloadOptions);
		runner.runInternal(params);
	}
}
