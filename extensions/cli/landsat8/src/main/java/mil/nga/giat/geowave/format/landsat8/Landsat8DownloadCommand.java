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
package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "download", parentOperation = Landsat8Section.class)
@Parameters(commandDescription = "Download Landsat 8 imagery to a local directory")
public class Landsat8DownloadCommand extends
		DefaultOperation implements
		Command
{

	@ParametersDelegate
	protected Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();

	@ParametersDelegate
	protected Landsat8DownloadCommandLineOptions downloadOptions = new Landsat8DownloadCommandLineOptions();

	public Landsat8DownloadCommand() {}

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
