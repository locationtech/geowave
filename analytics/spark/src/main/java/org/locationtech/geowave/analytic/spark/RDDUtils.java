/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.geotools.geometry.jts.JTS;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ScaledTemporalRange;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.operation.predicate.RectangleIntersects;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class RDDUtils {

  private static Logger LOGGER = LoggerFactory.getLogger(RDDUtils.class);

  /**
   * Translate a set of objects in a JavaRDD to SimpleFeatures and push to GeoWave
   *
   * @throws IOException
   */
  public static void writeRDDToGeoWave(
      final SparkContext sc,
      final Index index,
      final DataStorePluginOptions outputStoreOptions,
      final DataTypeAdapter adapter,
      final GeoWaveRDD inputRDD) throws IOException {
    if (!inputRDD.isLoaded()) {
      LOGGER.error("Must provide a loaded RDD.");
      return;
    }

    writeToGeoWave(sc, index, outputStoreOptions, adapter, inputRDD.getRawRDD().values());
  }

  public static void writeRDDToGeoWave(
      final SparkContext sc,
      final Index[] indices,
      final DataStorePluginOptions outputStoreOptions,
      final DataTypeAdapter adapter,
      final GeoWaveRDD inputRDD) throws IOException {
    if (!inputRDD.isLoaded()) {
      LOGGER.error("Must provide a loaded RDD.");
      return;
    }

    for (int iStrategy = 0; iStrategy < indices.length; iStrategy += 1) {
      writeToGeoWave(
          sc,
          indices[iStrategy],
          outputStoreOptions,
          adapter,
          inputRDD.getRawRDD().values());
    }
  }

  public static JavaRDD<Point> rddFeatureCentroids(final GeoWaveRDD inputRDD) {
    if (!inputRDD.isLoaded()) {
      LOGGER.error("Must provide a loaded RDD.");
      return null;
    }
    final JavaRDD<Point> centroids = inputRDD.getRawRDD().values().map(feature -> {
      final Geometry geom = (Geometry) feature.getDefaultGeometry();
      return geom.getCentroid();
    });

    return centroids;
  }

  public static JavaRDD<Vector> rddFeatureVectors(final GeoWaveRDD inputRDD) {

    return rddFeatureVectors(inputRDD, null, null);
  }

  public static JavaRDD<Vector> rddFeatureVectors(
      final GeoWaveRDD inputRDD,
      final String timeField,
      final ScaledTemporalRange scaledRange) {
    if (!inputRDD.isLoaded()) {
      LOGGER.error("Must provide a loaded RDD.");
      return null;
    }
    final JavaRDD<Vector> vectorRDD = inputRDD.getRawRDD().values().map(feature -> {
      final Point centroid = ((Geometry) feature.getDefaultGeometry()).getCentroid();

      int numValues = 2;
      Date time = null;

      if (timeField != null) {
        // if this is a ranged schema, we have to take the
        // midpoint
        if (timeField.contains("|")) {
          final int pipeIndex = timeField.indexOf("|");
          final String startField = timeField.substring(0, pipeIndex);
          final String endField = timeField.substring(pipeIndex + 1);

          final Date start = (Date) feature.getAttribute(startField);
          final Date end = (Date) feature.getAttribute(endField);

          final long halfDur = (end.getTime() - start.getTime()) / 2;

          time = new Date(start.getTime() + halfDur);
        } else {
          time = (Date) feature.getAttribute(timeField);
        }

        if (time != null) {
          numValues++;
        }
      }

      final double[] values = new double[numValues];
      values[0] = centroid.getX();
      values[1] = centroid.getY();

      if (time != null) {
        values[2] = scaledRange.timeToValue(time);
      }

      return Vectors.dense(values);
    });

    return vectorRDD;
  }

  public static InsertionIds trimIndexIds(
      final InsertionIds rawIds,
      final Geometry geom,
      final NumericIndexStrategy index) {
    for (final SinglePartitionInsertionIds insertionId : rawIds.getPartitionKeys()) {
      final byte[] partitionKey = insertionId.getPartitionKey();
      final int size = insertionId.getSortKeys().size();
      if (size > 3) {
        final Iterator<byte[]> it = insertionId.getSortKeys().iterator();
        while (it.hasNext()) {
          final byte[] sortKey = it.next();
          final MultiDimensionalNumericData keyTile = index.getRangeForId(partitionKey, sortKey);
          final Envelope other = new Envelope();
          other.init(
              keyTile.getMinValuesPerDimension()[0],
              keyTile.getMaxValuesPerDimension()[0],
              keyTile.getMinValuesPerDimension()[1],
              keyTile.getMaxValuesPerDimension()[1]);
          final Polygon rect = JTS.toGeometry(other);
          if (!RectangleIntersects.intersects(rect, geom)) {
            it.remove();
          }
        }
      }
    }
    return rawIds;
  }

  /**
   * Translate a set of objects in a JavaRDD to a provided type and push to GeoWave
   *
   * @throws IOException
   */
  private static void writeToGeoWave(
      final SparkContext sc,
      final Index index,
      final DataStorePluginOptions outputStoreOptions,
      final DataTypeAdapter adapter,
      final JavaRDD<SimpleFeature> inputRDD) throws IOException {

    // setup the configuration and the output format
    final Configuration conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration());

    GeoWaveOutputFormat.setStoreOptions(conf, outputStoreOptions);
    GeoWaveOutputFormat.addIndex(conf, index);
    GeoWaveOutputFormat.addDataAdapter(conf, adapter);

    // create the job
    final Job job = new Job(conf);
    job.setOutputKeyClass(GeoWaveOutputKey.class);
    job.setOutputValueClass(SimpleFeature.class);
    job.setOutputFormatClass(GeoWaveOutputFormat.class);

    // broadcast string names
    final ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
    final Broadcast<String> typeName = sc.broadcast(adapter.getTypeName(), stringTag);
    final Broadcast<String> indexName = sc.broadcast(index.getName(), stringTag);

    // map to a pair containing the output key and the output value
    inputRDD.mapToPair(
        feat -> new Tuple2<>(
            new GeoWaveOutputKey(typeName.value(), indexName.value()),
            feat)).saveAsNewAPIHadoopDataset(job.getConfiguration());
  }

  public static void writeRasterToGeoWave(
      final SparkContext sc,
      final Index index,
      final DataStorePluginOptions outputStoreOptions,
      final RasterDataAdapter adapter,
      final JavaRDD<GridCoverage> inputRDD) throws IOException {

    // setup the configuration and the output format
    final Configuration conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration());

    GeoWaveOutputFormat.setStoreOptions(conf, outputStoreOptions);
    GeoWaveOutputFormat.addIndex(conf, index);
    GeoWaveOutputFormat.addDataAdapter(conf, adapter);

    // create the job
    final Job job = new Job(conf);
    job.setOutputKeyClass(GeoWaveOutputKey.class);
    job.setOutputValueClass(GridCoverage.class);
    job.setOutputFormatClass(GeoWaveOutputFormat.class);

    // broadcast string names
    final ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
    final Broadcast<String> typeName = sc.broadcast(adapter.getTypeName(), stringTag);
    final Broadcast<String> indexName = sc.broadcast(index.getName(), stringTag);

    // map to a pair containing the output key and the output value
    inputRDD.mapToPair(
        gridCoverage -> new Tuple2<>(
            new GeoWaveOutputKey(typeName.value(), indexName.value()),
            gridCoverage)).saveAsNewAPIHadoopDataset(job.getConfiguration());
  }

  public static Broadcast<? extends NumericIndexStrategy> broadcastIndexStrategy(
      final SparkContext sc,
      final NumericIndexStrategy indexStrategy) {
    final ClassTag<NumericIndexStrategy> indexClassTag =
        scala.reflect.ClassTag$.MODULE$.apply(indexStrategy.getClass());
    final Broadcast<NumericIndexStrategy> broadcastStrategy =
        sc.broadcast(indexStrategy, indexClassTag);
    return broadcastStrategy;
  }
}
