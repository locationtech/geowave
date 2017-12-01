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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.BasicIndexOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

@Parameters(commandDescription = "Configure an index for usage in GeoWave")
public class AddSpatialIndexCommand extends
		ServiceEnabledCommand<String>
{
	/**
	 * A REST Operation for the AddIndexCommand where --type=spatial
	 */

	@Parameter(description = "<name>", required = true)
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default index creating stores")
	private Boolean makeDefault;

	private ServiceStatus status = ServiceStatus.OK;

	@ParametersDelegate
	private final BasicIndexOptions basicIndexOptions = new BasicIndexOptions();

	private IndexPluginOptions pluginOptions = new IndexPluginOptions();

	@ParametersDelegate
	SpatialOptions opts = new SpatialOptions();

	@Override
	public boolean prepare(
			final OperationParams params ) {

		pluginOptions.selectPlugin("spatial");
		pluginOptions.setBasicIndexOptions(basicIndexOptions);
		pluginOptions.setDimensionalityTypeOptions(opts);
		return true;
	}

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}

	@Override
	public String getId() {
		return ConfigSection.class.getName() + ".addindex/spatial";
	}

	@Override
	public String getPath() {
		return "v0/config/addindex/spatial";
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
		return "spatial";
	}

	public void setPluginOptions(
			final IndexPluginOptions pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}

	public ServiceStatus getStatus() {
		return status;
	}

	public void setStatus(
			ServiceStatus status ) {
		this.status = status;
	}

	@Override
	public Pair<ServiceStatus, String> executeService(
			OperationParams params )
			throws Exception {
		String ret = computeResults(params);
		return ImmutablePair.of(
				status,
				ret);
	}

	@Override
	public String computeResults(
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

		final File propFile = getGeoWaveConfigFile(params);

		final Properties existingProps = ConfigOptions.loadProperties(propFile);

		// Make sure we're not already in the index.
		final IndexPluginOptions existPlugin = new IndexPluginOptions();
		if (existPlugin.load(
				existingProps,
				getNamespace())) {
			setStatus(ServiceStatus.DUPLICATE);
			return "That index already exists: " + getPluginName();
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

		StringBuilder builder = new StringBuilder();
		for (Object key : existingProps.keySet()) {
			String[] split = key.toString().split(
					"\\.");
			if (split.length > 1) {
				if (split[1].equals(parameters.get(0))) {
					builder.append(key.toString() + "=" + existingProps.getProperty(key.toString()) + "\n");
				}
			}
		}
		setStatus(ServiceStatus.OK);
		return builder.toString();
	}
}
