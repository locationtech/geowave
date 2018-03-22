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
package mil.nga.giat.geowave.adapter.vector.export;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.adapter.vector.cli.VectorSection;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;

@GeowaveOperation(name = "mrexport", parentOperation = VectorSection.class)
@Parameters(commandDescription = "Export data using map-reduce")
public class VectorMRExportCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<path to base directory to write to> <store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VectorMRExportOptions mrOptions = new VectorMRExportOptions();

	private DataStorePluginOptions storeOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		createRunner(
				params).runJob();
	}

	public VectorMRExportJobRunner createRunner(
			OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <path to base directory to write to> <store name>");
		}

		String hdfsPath = parameters.get(0);
		String storeName = parameters.get(1);

		// Config file
		File configFile = getGeoWaveConfigFile(params);
		Properties configProperties = ConfigOptions.loadProperties(configFile);
		String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);

		// Attempt to load store.
		if (storeOptions == null) {
			StoreLoader storeLoader = new StoreLoader(
					storeName);
			if (!storeLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + storeLoader.getStoreName());
			}
			storeOptions = storeLoader.getDataStorePlugin();
		}

		VectorMRExportJobRunner vectorRunner = new VectorMRExportJobRunner(
				storeOptions,
				mrOptions,
				hdfsHostPort,
				hdfsPath);
		return vectorRunner;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String hdfsPath,
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(hdfsPath);
		this.parameters.add(storeName);
	}

	public VectorMRExportOptions getMrOptions() {
		return mrOptions;
	}

	public void setMrOptions(
			VectorMRExportOptions mrOptions ) {
		this.mrOptions = mrOptions;
	}

	public DataStorePluginOptions getStoreOptions() {
		return storeOptions;
	}

	public void setStoreOptions(
			DataStorePluginOptions storeOptions ) {
		this.storeOptions = storeOptions;
	}
}
