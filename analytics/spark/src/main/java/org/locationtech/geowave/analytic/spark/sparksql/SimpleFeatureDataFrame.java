/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomFunctionRegistry;
import org.locationtech.geowave.analytic.spark.sparksql.util.SchemaConverter;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFeatureDataFrame {
  private static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureDataFrame.class);

  private final SparkSession sparkSession;
  private SimpleFeatureType featureType;
  private StructType schema;
  private JavaRDD<Row> rowRDD = null;
  private Dataset<Row> dataFrame = null;

  public SimpleFeatureDataFrame(final SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public boolean init(final DataStorePluginOptions dataStore, final String typeName) {
    featureType = FeatureDataUtils.getFeatureType(dataStore, typeName);
    if (featureType == null) {
      return false;
    }

    schema = SchemaConverter.schemaFromFeatureType(featureType);
    if (schema == null) {
      return false;
    }

    GeomFunctionRegistry.registerGeometryFunctions(sparkSession);

    return true;
  }

  public SimpleFeatureType getFeatureType() {
    return featureType;
  }

  public StructType getSchema() {
    return schema;
  }

  public JavaRDD<Row> getRowRDD() {
    return rowRDD;
  }

  public Dataset<Row> getDataFrame(final GeoWaveRDD pairRDD) {
    if (rowRDD == null) {
      final SimpleFeatureMapper mapper = new SimpleFeatureMapper(schema);

      rowRDD = pairRDD.getRawRDD().values().map(mapper);
    }

    if (dataFrame == null) {
      dataFrame = sparkSession.createDataFrame(rowRDD, schema);
    }

    return dataFrame;
  }

  public Dataset<Row> resetDataFrame(final GeoWaveRDD pairRDD) {
    rowRDD = null;
    dataFrame = null;

    return getDataFrame(pairRDD);
  }
}
