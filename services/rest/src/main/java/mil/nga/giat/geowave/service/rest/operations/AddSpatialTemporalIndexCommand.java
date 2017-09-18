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
package mil.nga.giat.geowave.service.rest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalOptions;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexCommand;
import mil.nga.giat.geowave.core.store.operations.remote.options.BasicIndexOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

@Parameters(commandDescription = "Configure an index for usage in GeoWave")
public class AddSpatialTemporalIndexCommand extends
		ServiceEnabledCommand<Void>
{
	/**
	 * A REST Operation for the AddIndexCommand where --type=spatial_temporal
	 */

	private final static Logger LOGGER = LoggerFactory.getLogger(AddIndexCommand.class);

	@Parameter(description = "<name>", required = true)
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default index creating stores")
	private Boolean makeDefault;

	@ParametersDelegate
	private final BasicIndexOptions basicIndexOptions = new BasicIndexOptions();

	private IndexPluginOptions pluginOptions = new IndexPluginOptions();

	@ParametersDelegate
	SpatialTemporalOptions opts = new SpatialTemporalOptions();

	@Override
	public boolean prepare(
			final OperationParams params ) {

		pluginOptions.selectPlugin("spatial_temporal");
		pluginOptions.setBasicIndexOptions(basicIndexOptions);
		pluginOptions.setDimensionalityTypeOptions(opts);
		// Successfully prepared.
		return true;
	}

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}

	@Override
	public String getId() {
		return ConfigSection.class.getName() + ".addindex/spatial_temporal";
	}

	@Override
	public String getPath() {
		return "config/addindex/spatial_temporal";
	}

	public IndexPluginOptions getPluginOptions() {
		return pluginOptions;
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return IndexPluginOptions.getIndexNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String indexName ) {
		parameters = new ArrayList<String>();
		parameters.add(indexName);
	}

	public Boolean getMakeDefault() {
		return makeDefault;
	}

	public void setMakeDefault(
			final Boolean makeDefault ) {
		this.makeDefault = makeDefault;
	}

	public String getType() {
		return "spatial_temporal";
	}

	public void setPluginOptions(
			final IndexPluginOptions pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}

	@Override
	public Void computeResults(
			final OperationParams params ) {

		// Ensure that a name is chosen.
		if (getParameters().size() < 1) {
			System.out.println(getParameters());
			throw new ParameterException(
					"Must specify index name");
		}

		if (getType() == null) {
			throw new ParameterException(
					"No type could be infered");
		}

		final File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		final Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

		// Make sure we're not already in the index.
		final IndexPluginOptions existPlugin = new IndexPluginOptions();
		if (existPlugin.load(
				existingProps,
				getNamespace())) {
			throw new ParameterException(
					"That index already exists: " + getPluginName());
		}

		pluginOptions.save(
				existingProps,
				getNamespace());

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {

			existingProps.setProperty(
					IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					getPluginName());
		}

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);

		return null;
	}
}
