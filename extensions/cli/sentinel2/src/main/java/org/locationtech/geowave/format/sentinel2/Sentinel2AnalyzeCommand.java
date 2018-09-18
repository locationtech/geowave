/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.format.sentinel2;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.format.sentinel2.AnalyzeRunner;
import org.locationtech.geowave.format.sentinel2.Sentinel2BasicCommandLineOptions;
import org.locationtech.geowave.format.sentinel2.Sentinel2Section;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "analyze", parentOperation = Sentinel2Section.class)
@Parameters(commandDescription = "Print out basic aggregate statistics for available Sentinel2 imagery.")
public class Sentinel2AnalyzeCommand extends
		DefaultOperation implements
		Command
{
	@ParametersDelegate
	protected Sentinel2BasicCommandLineOptions analyzeOptions = new Sentinel2BasicCommandLineOptions();

	public Sentinel2AnalyzeCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		final AnalyzeRunner runner = new AnalyzeRunner(
				analyzeOptions);
		runner.runInternal(params);
	}
}
