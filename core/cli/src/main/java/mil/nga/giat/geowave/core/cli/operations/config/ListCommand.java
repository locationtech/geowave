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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

@GeowaveOperation(name = "list", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "List property name within cache")
public class ListCommand extends
		ServiceEnabledCommand<SortedMap<String, Object>>
{

	@Parameter(names = {
		"-f",
		"--filter"
	})
	private String filter;

	@Override
	public void execute(
			final OperationParams params ) {
		final Pair<String, SortedMap<String, Object>> list = getProperties(params);
		final String name = list.getKey();

		JCommander.getConsole().println(
				"PROPERTIES (" + name + ")");

		final SortedMap<String, Object> properties = list.getValue();

		for (final Entry<String, Object> e : properties.entrySet()) {
			JCommander.getConsole().println(
					e.getKey() + ": " + e.getValue());
		}
	}

	@Override
	public SortedMap<String, Object> computeResults(
			final OperationParams params ) {

		return getProperties(
				params).getValue();
	}

	private Pair<String, SortedMap<String, Object>> getProperties(
			final OperationParams params ) {

		final File f = getGeoWaveConfigFile(params);

		// Reload options with filter if specified.
		Properties p = null;
		if (filter != null) {
			p = ConfigOptions.loadProperties(
					f,
					filter);
		}
		else {
			p = ConfigOptions.loadProperties(f);
		}
		return new ImmutablePair<>(
				f.getName(),
				new GeoWaveConfig(
						p));
	}

	protected static class GeoWaveConfig extends
			TreeMap<String, Object>
	{

		private static final long serialVersionUID = 1L;

		public GeoWaveConfig() {
			super();
		}

		public GeoWaveConfig(
				Map m ) {
			super(
					m);
		}

	}

}
