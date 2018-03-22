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
package mil.nga.giat.geowave.analytic.mapreduce.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.DBScanIterationsJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.operations.options.CommonOptions;
import mil.nga.giat.geowave.analytic.mapreduce.operations.options.DBScanOptions;
import mil.nga.giat.geowave.analytic.mapreduce.operations.options.PropertyManagementConverter;
import mil.nga.giat.geowave.analytic.param.ExtractParameters.Extract;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;

@GeowaveOperation(name = "dbscan", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "Density Based Scanner")
public class DBScanCommand extends
		ServiceEnabledCommand<Void>
{

	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private CommonOptions commonOptions = new CommonOptions();

	@ParametersDelegate
	private DBScanOptions dbScanOptions = new DBScanOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <storename>");
		}

		computeResults(params);
	}

	@Override
	public boolean runAsync() {
		return true;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName ) {
		parameters = new ArrayList<String>();
		parameters.add(storeName);
	}

	public CommonOptions getCommonOptions() {
		return commonOptions;
	}

	public void setCommonOptions(
			final CommonOptions commonOptions ) {
		this.commonOptions = commonOptions;
	}

	public DBScanOptions getDbScanOptions() {
		return dbScanOptions;
	}

	public void setDbScanOptions(
			final DBScanOptions dbScanOptions ) {
		this.dbScanOptions = dbScanOptions;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	@Override
	public Void computeResults(
			final OperationParams params )
			throws Exception {
		final String inputStoreName = parameters.get(0);

		// Config file
		final File configFile = getGeoWaveConfigFile(params);

		if (commonOptions.getMapReduceHdfsHostPort() == null) {

			Properties configProperties = ConfigOptions.loadProperties(configFile);
			String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
			commonOptions.setMapReduceHdfsHostPort(hdfsFSUrl);
		}

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Save a reference to the store in the property management.
		final PersistableStore persistedStore = new PersistableStore(
				inputStoreOptions);
		final PropertyManagement properties = new PropertyManagement();
		properties.store(
				StoreParameters.StoreParam.INPUT_STORE,
				persistedStore);

		// Convert properties from DBScanOptions and CommonOptions
		final PropertyManagementConverter converter = new PropertyManagementConverter(
				properties);
		converter.readProperties(commonOptions);
		converter.readProperties(dbScanOptions);
		properties.store(
				Extract.QUERY_OPTIONS,
				commonOptions.buildQueryOptions());

		final DBScanIterationsJobRunner runner = new DBScanIterationsJobRunner();
		final int status = runner.run(properties);
		if (status != 0) {
			throw new RuntimeException(
					"Failed to execute: " + status);
		}
		return null;
	}

}
