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

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2BasicCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2Section;
import mil.nga.giat.geowave.format.sentinel2.VectorIngestRunner;

@GeowaveOperation(name = "ingestvector", parentOperation = Sentinel2Section.class)
@Parameters(commandDescription = "Ingest routine for searching Sentinel2 scenes that match certain criteria and ingesting the scene and band metadata into GeoWave's vector store.")
public class Sentinel2IngestVectorCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<storename> <comma delimited index/group list>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	protected Sentinel2BasicCommandLineOptions analyzeOptions = new Sentinel2BasicCommandLineOptions();

	public Sentinel2IngestVectorCommand() {}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		final VectorIngestRunner runner = new VectorIngestRunner(
				analyzeOptions,
				parameters);
		runner.runInternal(params);
	}
}
