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
package org.locationtech.geowave.cli.debug;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.GeoWaveSparkConf;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.analytic.spark.spatial.SpatialJoinRunner;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "sparkcql", parentOperation = DebugSection.class)
@Parameters(commandDescription = "spark cql query")
public class SparkQuery extends
		AbstractGeoWaveQuery
{
	private static Logger LOGGER = LoggerFactory.getLogger(SparkQuery.class);

	@Parameter(names = "--cql", required = true, description = "CQL Filter executed client side")
	private String cqlStr;

	@Parameter(names = "--sparkMaster", description = "Spark Master")
	private String sparkMaster = "yarn";
	@Parameter(names = {
		"-n",
		"--name"
	}, description = "The spark application name")
	private String appName = "Spatial Join Spark";

	@Parameter(names = {
		"-ho",
		"--host"
	}, description = "The spark driver host")
	private String host = "localhost";

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final String typeName,
			final String indexName,
			final DataStore dataStore,
			final boolean debug,
			DataStorePluginOptions pluginOptions ) {
		String jar = "";
		try {
			jar = SpatialJoinRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
		}
		catch (final URISyntaxException e) {
			LOGGER.error(
					"Unable to set jar location in spark configuration",
					e);
		}
		SparkConf addonOptions = GeoWaveSparkConf.getDefaultConfig();
		addonOptions = addonOptions.setAppName(
				appName).setMaster(
				sparkMaster).set(
				"spark.jars",
				jar);

		if (!Objects.equals(
				sparkMaster,
				"yarn")) {
			addonOptions = addonOptions.set(
					"spark.driver.host",
					host);
		}

		SparkSession session = GeoWaveSparkConf.createDefaultSession(addonOptions);
		long count = 0;
		VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
		if (typeName != null) {
			bldr.addTypeName(typeName);
		}
		if (indexName != null) {
			bldr.indexName(indexName);
		}
		RDDOptions rddOptions = new RDDOptions();
		rddOptions.setQuery(bldr.constraints(
				bldr.constraintsFactory().cqlConstraints(
						cqlStr)).build());
		try {
			count = GeoWaveRDDLoader.loadRDD(
					session.sparkContext(),
					pluginOptions,
					rddOptions).getRawRDD().count();
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to load RDD",
					e);
		}
		return count;
	}
}
