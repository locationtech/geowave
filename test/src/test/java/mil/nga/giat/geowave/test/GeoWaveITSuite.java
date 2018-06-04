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
package mil.nga.giat.geowave.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

import mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT;
import mil.nga.giat.geowave.test.basic.GeoWaveBasicSpatialTemporalVectorIT;
import mil.nga.giat.geowave.test.basic.GeoWaveBasicSpatialVectorIT;
import mil.nga.giat.geowave.test.basic.GeoWaveVectorSerializationIT;
import mil.nga.giat.geowave.test.basic.GeowaveCustomCRSSpatialVectorIT;
import mil.nga.giat.geowave.test.basic.GeoWaveVisibilityIT;
import mil.nga.giat.geowave.test.config.ConfigCacheIT;
import mil.nga.giat.geowave.test.javaspark.GeowaveSparkIngestIT;
import mil.nga.giat.geowave.test.kafka.BasicKafkaIT;
import mil.nga.giat.geowave.test.landsat.LandsatIT;
import mil.nga.giat.geowave.test.mapreduce.BasicMapReduceIT;
import mil.nga.giat.geowave.test.mapreduce.BulkIngestInputGenerationIT;
import mil.nga.giat.geowave.test.mapreduce.DBScanIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveNNIT;
import mil.nga.giat.geowave.test.mapreduce.KDERasterResizeIT;
import mil.nga.giat.geowave.test.query.AttributesSubsetQueryIT;
import mil.nga.giat.geowave.test.query.PolygonDataIdQueryIT;
import mil.nga.giat.geowave.test.query.SpatialTemporalQueryIT;
import mil.nga.giat.geowave.test.services.ConfigServicesIT;
import mil.nga.giat.geowave.test.services.GeoServerIT;
import mil.nga.giat.geowave.test.services.GeoServerIngestIT;
import mil.nga.giat.geowave.test.spark.GeoWaveJavaSparkIT;
import mil.nga.giat.geowave.test.spark.GeoWaveJavaSparkKMeansIT;
import mil.nga.giat.geowave.test.spark.GeoWaveJavaSparkSQLIT;
import mil.nga.giat.geowave.test.spark.GeoWaveSparkSpatialJoinIT;

@RunWith(GeoWaveITSuiteRunner.class)
@SuiteClasses({
	GeoWaveBasicSpatialVectorIT.class,
	GeowaveCustomCRSSpatialVectorIT.class,
	GeoWaveBasicSpatialTemporalVectorIT.class,
	GeoWaveVectorSerializationIT.class,
	BasicKafkaIT.class,
	BasicMapReduceIT.class,
	LandsatIT.class,
	GeoWaveVisibilityIT.class,
	GeoWaveBasicRasterIT.class,
	KDERasterResizeIT.class,
	BulkIngestInputGenerationIT.class,
	GeoWaveNNIT.class,
	AttributesSubsetQueryIT.class,
	SpatialTemporalQueryIT.class,
	PolygonDataIdQueryIT.class,
	ConfigCacheIT.class,
	DBScanIT.class,
	GeoWaveJavaSparkIT.class,
	GeoWaveJavaSparkKMeansIT.class,
	GeoWaveJavaSparkSQLIT.class,
	GeoWaveSparkSpatialJoinIT.class,
	ConfigServicesIT.class,
	GeoServerIngestIT.class,
	GeoServerIT.class
})
public class GeoWaveITSuite
{
	@BeforeClass
	public static void setupSuite() {
		synchronized (GeoWaveITRunner.MUTEX) {
			GeoWaveITRunner.DEFER_CLEANUP.set(true);
		}
	}

	@AfterClass
	public static void tearDownSuite() {
		synchronized (GeoWaveITRunner.MUTEX) {
			GeoWaveITRunner.DEFER_CLEANUP.set(false);
		}
	}
}
