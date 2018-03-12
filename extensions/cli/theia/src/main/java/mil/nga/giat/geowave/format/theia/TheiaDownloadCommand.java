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

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.format.theia.DownloadRunner;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaDownloadCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaSection;

@GeowaveOperation(name = "download", parentOperation = TheiaSection.class)
@Parameters(commandDescription = "Download Theia imagery to a local directory.")
public class TheiaDownloadCommand extends
		DefaultOperation implements
		Command
{
	@ParametersDelegate
	protected TheiaBasicCommandLineOptions analyzeOptions = new TheiaBasicCommandLineOptions();

	@ParametersDelegate
	protected TheiaDownloadCommandLineOptions downloadOptions = new TheiaDownloadCommandLineOptions();

	public TheiaDownloadCommand() {}

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
