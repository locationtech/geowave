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
package org.locationtech.geowave.analytic.spark.spatial.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.mapreduce.operations.AnalyticSection;
import org.locationtech.geowave.analytic.mapreduce.operations.options.PropertyManagementConverter;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomFunction;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomWithinDistance;
import org.locationtech.geowave.analytic.spark.sparksql.udf.UDFRegistrySPI;
import org.locationtech.geowave.analytic.spark.sparksql.udf.UDFRegistrySPI.UDFNameAndConstructor;
import org.locationtech.geowave.analytic.spark.spatial.SpatialJoinRunner;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "spatialjoin", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "Spatial Join using Spark ")
public class SpatialJoinCommand extends
		ServiceEnabledCommand<Void>
{
	@Parameter(description = "<left storename> <right storename> <output storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private SpatialJoinCmdOptions spatialJoinOptions = new SpatialJoinCmdOptions();

	DataStorePluginOptions leftDataStore = null;
	DataStorePluginOptions rightDataStore = null;
	DataStorePluginOptions outputDataStore = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <left storename> <right storename> <output storename>");
		}
		computeResults(params);
	}

	@Override
	public Void computeResults(
			OperationParams params )
			throws Exception {
		final String leftStoreName = parameters.get(0);
		final String rightStoreName = parameters.get(1);
		final String outputStoreName = parameters.get(2);

		// Config file
		final File configFile = getGeoWaveConfigFile(params);

		// Attempt to load stores.
		if (leftDataStore == null) {
			leftDataStore = this.loadStore(
					leftStoreName,
					configFile);
		}

		if (rightDataStore == null) {
			rightDataStore = this.loadStore(
					rightStoreName,
					configFile);
		}

		if (outputDataStore == null) {
			outputDataStore = this.loadStore(
					outputStoreName,
					configFile);
		}

		// Save a reference to the output store in the property management.
		final PersistableStore persistedStore = new PersistableStore(
				outputDataStore);
		final PropertyManagement properties = new PropertyManagement();
		properties.store(
				StoreParameters.StoreParam.OUTPUT_STORE,
				persistedStore);
		// Convert properties from DBScanOptions and CommonOptions
		final PropertyManagementConverter converter = new PropertyManagementConverter(
				properties);
		converter.readProperties(spatialJoinOptions);

		// TODO: Create GeomPredicate function from name
		UDFNameAndConstructor udfFunc = UDFRegistrySPI.findFunctionByName(spatialJoinOptions.getPredicate());
		if (udfFunc == null) {
			throw new ParameterException(
					"UDF function matching " + spatialJoinOptions.getPredicate() + " not found.");
		}

		GeomFunction predicate = udfFunc.getPredicateConstructor().get();

		// Special case for distance function since it takes a scalar radius.
		if (predicate instanceof GeomWithinDistance) {
			((GeomWithinDistance) predicate).setRadius(spatialJoinOptions.getRadius());
		}

		final SpatialJoinRunner runner = new SpatialJoinRunner();
		runner.setAppName(spatialJoinOptions.getAppName());
		runner.setMaster(spatialJoinOptions.getMaster());
		runner.setHost(spatialJoinOptions.getHost());
		runner.setPartCount(spatialJoinOptions.getPartCount());

		runner.setPredicate(predicate);

		// set DataStore options for runner
		runner.setLeftStore(leftDataStore);
		if (spatialJoinOptions.getLeftAdapterTypeName() != null) {
			runner.setLeftAdapterTypeName(spatialJoinOptions.getLeftAdapterTypeName());
		}

		runner.setRightStore(rightDataStore);
		if (spatialJoinOptions.getRightAdapterTypeName() != null) {
			runner.setRightAdapterTypeName(spatialJoinOptions.getRightAdapterTypeName());
		}

		runner.setOutputStore(outputDataStore);
		if (spatialJoinOptions.getOutputLeftAdapterTypeName() != null) {
			runner.setOutputLeftAdapterTypeName(spatialJoinOptions.getOutputLeftAdapterTypeName());
		}

		if (spatialJoinOptions.getOutputRightAdapterTypeName() != null) {
			runner.setOutputRightAdapterTypeName(spatialJoinOptions.getOutputRightAdapterTypeName());
		}

		// Finally call run to execute the join
		runner.run();
		runner.close();
		return null;
	}

	private DataStorePluginOptions loadStore(
			String storeName,
			File configFile ) {
		final StoreLoader storeLoader = new StoreLoader(
				storeName);
		if (!storeLoader.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find left store: " + storeLoader.getStoreName());
		}
		return storeLoader.getDataStorePlugin();
	}

}
