/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import java.io.IOException;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * This class is intended to provide a self-contained, easy-to-follow example of how a custom
 * statistic can be created an used. The example statistic is a word count statistic that can count
 * the number of words in a string field across an entire data set.
 */
public class CustomStatisticExample {
  private DataStore dataStore;
  private SimpleFeatureType simpleFeatureType;
  private FeatureDataAdapter adapter;
  private Index spatialIndex;

  public static void main(final String[] args) throws IOException, CQLException {

    final CustomStatisticExample example = new CustomStatisticExample();
    example.run();
  }

  public void run() {
    // Create an in-memory data store to use with this example
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    // Create the simple feature type for our data
    simpleFeatureType = getSimpleFeatureType();

    // Create an adapter for our features
    adapter = new FeatureDataAdapter(simpleFeatureType);

    // Create the spatial index
    spatialIndex = new SpatialIndexBuilder().createIndex();

    // Add the type to the data store with the spatial and custom indices
    dataStore.addType(adapter, spatialIndex);

    // Create a word count statistic on the `str` field of our type for all words
    final WordCountStatistic allWords = new WordCountStatistic();
    allWords.setTypeName(adapter.getTypeName());
    allWords.setFieldName("str");
    allWords.setMinWordLength(0);
    allWords.setTag("ALL_WORDS");

    // Create a word count statistic on the `str` field of our type for long words
    final WordCountStatistic longWords = new WordCountStatistic();
    longWords.setTypeName(adapter.getTypeName());
    longWords.setFieldName("str");
    longWords.setMinWordLength(5);
    longWords.setTag("LONG_WORDS");

    // Add the statistics
    dataStore.addStatistic(allWords, longWords);

    // Ingest the data into a spatial index
    ingestData();

    // Get the statistics
    System.out.println("Total number of words: " + dataStore.getStatisticValue(allWords));
    System.out.println("Total number of long words: " + dataStore.getStatisticValue(longWords));

    // You can also get the actual statistics from the data store at a later time
    final WordCountStatistic stat =
        (WordCountStatistic) dataStore.getFieldStatistic(
            WordCountStatistic.STATS_TYPE,
            adapter.getTypeName(),
            "str",
            "ALL_WORDS");
    System.out.println("ALL_WORDS Statistic: " + stat.toString());

  }

  public void ingestData() {
    // Create features with string fields of various word lengths
    try (Writer<SimpleFeature> writer = dataStore.createWriter(adapter.getTypeName())) {
      writer.write(buildSimpleFeature("feature1", new Coordinate(0, 0), "a set of words"));
      writer.write(buildSimpleFeature("feature2", new Coordinate(1, 1), "another set of words"));
      writer.write(buildSimpleFeature("feature3", new Coordinate(2, 2), "two words"));
      writer.write(buildSimpleFeature("feature4", new Coordinate(3, 3), "word"));
      writer.write(
          buildSimpleFeature(
              "feature5",
              new Coordinate(4, 4),
              "a long string with quite a few words to count"));
    }

  }

  private SimpleFeatureType getSimpleFeatureType() {
    final String NAME = "ExampleType";
    final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();
    sftBuilder.setName(NAME);
    sftBuilder.add(atBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
    sftBuilder.add(atBuilder.binding(String.class).nillable(false).buildDescriptor("str"));

    return sftBuilder.buildFeatureType();
  }

  private SimpleFeature buildSimpleFeature(
      final String featureId,
      final Coordinate coordinate,
      final String str) {

    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(simpleFeatureType);
    builder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));
    builder.set("str", str);

    return builder.buildFeature(featureId);
  }
}
