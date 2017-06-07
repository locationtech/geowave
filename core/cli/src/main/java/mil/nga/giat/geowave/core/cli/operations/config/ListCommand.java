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
package mil.nga.giat.geowave.core.cli.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "list", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "List property name within cache")
public class ListCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(names = {
		"-f",
		"--filter"
	})
	private String filter;

	@Override
	public void execute(
			OperationParams params ) {

		File f = getGeoWaveConfigFile(params);

		// Reload options with filter if specified.
		Properties p = getGeoWaveConfigProperties(
				params,
				filter);

		JCommander.getConsole().println(
				"PROPERTIES (" + f.getName() + ")");

		List<String> keys = new ArrayList<String>();
		keys.addAll(p.stringPropertyNames());
		Collections.sort(keys);

		for (String key : keys) {
			String value = (String) p.get(key);
			JCommander.getConsole().println(
					key + ": " + value);
		}
	}
}
