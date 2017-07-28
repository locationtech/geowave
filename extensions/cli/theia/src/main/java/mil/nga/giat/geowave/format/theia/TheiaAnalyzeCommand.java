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
import mil.nga.giat.geowave.format.theia.AnalyzeRunner;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaSection;

@GeowaveOperation(name = "analyze", parentOperation = TheiaSection.class)
@Parameters(commandDescription = "Print out basic aggregate statistics for available Theia imagery.")
public class TheiaAnalyzeCommand extends
		DefaultOperation implements
		Command
{
	@ParametersDelegate
	protected TheiaBasicCommandLineOptions theiaOptions = new TheiaBasicCommandLineOptions();

	public TheiaAnalyzeCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		final AnalyzeRunner runner = new AnalyzeRunner(
				theiaOptions);
		runner.runInternal(params);
	}
}
