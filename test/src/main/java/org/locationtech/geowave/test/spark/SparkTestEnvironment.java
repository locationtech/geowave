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
package org.locationtech.geowave.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.geowave.analytic.spark.GeoWaveSparkConf;
import org.locationtech.geowave.test.TestEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkTestEnvironment implements
		TestEnvironment
{

	private static final Logger LOGGER = LoggerFactory.getLogger(SparkTestEnvironment.class);

	private static SparkTestEnvironment singletonInstance = null;
	protected SparkSession defaultSession = null;

	public static synchronized SparkTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new SparkTestEnvironment();
		}
		return singletonInstance;
	}

	@Override
	public void setup()
			throws Exception {
		if (defaultSession == null) {
			SparkConf addonOptions = new SparkConf();
			addonOptions.setMaster("local[*]");
			addonOptions.setAppName("CoreGeoWaveSparkITs");
			defaultSession = GeoWaveSparkConf.createDefaultSession(addonOptions);
			if (defaultSession == null) {
				LOGGER.error("Unable to create default spark session for tests");
				return;
			}
		}
	}

	@Override
	public void tearDown()
			throws Exception {
		if (defaultSession != null) {
			defaultSession.close();
			defaultSession = null;
		}
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

	public SparkSession getDefaultSession() {
		return defaultSession;
	}
}
