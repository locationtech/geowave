/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.locationtech.geowave.mapreduce.splits.SplitsProviderIT;
import org.locationtech.geowave.test.basic.GeoWaveBasicCustomCRSRasterIT;
import org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT;
import org.locationtech.geowave.test.basic.GeoWaveBasicSpatialTemporalVectorIT;
import org.locationtech.geowave.test.basic.GeoWaveBasicTemporalVectorIT;
import org.locationtech.geowave.test.basic.GeoWaveCustomCRSSpatialVectorIT;
import org.locationtech.geowave.test.basic.GeoWaveCustomIndexIT;
import org.locationtech.geowave.test.basic.GeoWaveGeometryPrecisionIT;
import org.locationtech.geowave.test.basic.GeoWaveVectorSerializationIT;
import org.locationtech.geowave.test.basic.GeoWaveVisibilityIT;
import org.locationtech.geowave.test.config.ConfigCacheIT;
import org.locationtech.geowave.test.docs.GeoWaveDocumentationExamplesIT;
import org.locationtech.geowave.test.kafka.BasicKafkaIT;
import org.locationtech.geowave.test.landsat.CustomCRSLandsatIT;
import org.locationtech.geowave.test.mapreduce.BasicMapReduceIT;
import org.locationtech.geowave.test.mapreduce.BulkIngestInputGenerationIT;
import org.locationtech.geowave.test.mapreduce.CustomCRSKDERasterResizeIT;
import org.locationtech.geowave.test.query.AttributesSubsetQueryIT;
import org.locationtech.geowave.test.query.PolygonDataIdQueryIT;
import org.locationtech.geowave.test.query.QueryOptionsIT;
import org.locationtech.geowave.test.query.SpatialTemporalQueryIT;
import org.locationtech.geowave.test.secondary.BasicSecondaryIndexIT;
import org.locationtech.geowave.test.secondary.DataIndexOnlyIT;
import org.locationtech.geowave.test.secondary.MapReduceSecondaryIndexIT;
import org.locationtech.geowave.test.secondary.SimpleQuerySecondaryIndexIT;
import org.locationtech.geowave.test.secondary.VisibilitySecondaryIndexIT;
import org.locationtech.geowave.test.services.ConfigServicesIT;
import org.locationtech.geowave.test.services.FileUploadIT;
import org.locationtech.geowave.test.services.GeoServerIT;
import org.locationtech.geowave.test.services.GeoServerIngestIT;
import org.locationtech.geowave.test.services.IndexServicesIT;
import org.locationtech.geowave.test.services.IngestIT;
import org.locationtech.geowave.test.services.StatServicesIT;
import org.locationtech.geowave.test.services.StoreServicesIT;
import org.locationtech.geowave.test.services.TypeServicesIT;
import org.locationtech.geowave.test.services.grpc.GeoWaveGrpcIT;
import org.locationtech.geowave.test.spark.GeoWaveBasicSparkIT;
import org.locationtech.geowave.test.spark.GeoWaveSparkKMeansIT;
import org.locationtech.geowave.test.spark.GeoWaveSparkSQLIT;
import org.locationtech.geowave.test.spark.GeoWaveSparkSpatialJoinIT;
import org.locationtech.geowave.test.stability.GeoWaveStabilityIT;

@RunWith(GeoWaveITSuiteRunner.class)
@SuiteClasses({
    GeoWaveVisibilityIT.class,
    GeoWaveCustomCRSSpatialVectorIT.class,
    GeoWaveBasicTemporalVectorIT.class,
    GeoWaveBasicSpatialTemporalVectorIT.class,
    GeoWaveGeometryPrecisionIT.class,
    GeoWaveCustomIndexIT.class,
    GeoWaveDocumentationExamplesIT.class,
    // TODO need to mock up S3
    // GeowaveBasicURLIngestIT.class,
    GeoWaveVectorSerializationIT.class,
    BasicKafkaIT.class,
    BasicMapReduceIT.class,
    SimpleQuerySecondaryIndexIT.class,
    BasicSecondaryIndexIT.class,
    DataIndexOnlyIT.class,
    MapReduceSecondaryIndexIT.class,
    VisibilitySecondaryIndexIT.class,
    GeoWaveStabilityIT.class,
    QueryOptionsIT.class,
    // for now tests are taking too long in travis and landsatIT is a long test
    // LandsatIT.class,
    // these also help shave off some time
    // DBScanIT.class,
    // GeoWaveNNIT.class,
    CustomCRSLandsatIT.class,
    GeoWaveBasicRasterIT.class,
    GeoWaveBasicCustomCRSRasterIT.class,
    BulkIngestInputGenerationIT.class,
    AttributesSubsetQueryIT.class,
    SpatialTemporalQueryIT.class,
    PolygonDataIdQueryIT.class,
    ConfigCacheIT.class,
    GeoWaveBasicSparkIT.class,
    GeoWaveSparkKMeansIT.class,
    GeoWaveSparkSQLIT.class,
    GeoWaveSparkSpatialJoinIT.class,
    GeoServerIT.class,
    GeoServerIngestIT.class,
    // has to be after SparkEnvironment usage
    CustomCRSKDERasterResizeIT.class,
    GeoWaveGrpcIT.class,
    ConfigServicesIT.class,
    StoreServicesIT.class,
    IndexServicesIT.class,
    StatServicesIT.class,
    TypeServicesIT.class,
    IngestIT.class,
    FileUploadIT.class,
    SplitsProviderIT.class})
public class GeoWaveITSuite {
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
