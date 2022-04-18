/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.ingest.bulk;

import java.io.IOException;
import java.util.List;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloKeyValuePairGenerator;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.feature.simple.SimpleFeature;

public class SimpleFeatureToAccumuloKeyValueMapper extends Mapper<LongWritable, Text, Key, Value> {

  private final DataTypeAdapter<SimpleFeature> adapter =
      new FeatureDataAdapter(GeonamesSimpleFeatureType.getInstance());
  // this is not the most robust way to assign an internal adapter ID
  // but is simple and will work in a majority of cases
  private final InternalDataAdapter<SimpleFeature> internalAdapter =
      adapter.asInternalAdapter(
          InternalAdapterStoreImpl.getLazyInitialAdapterId(adapter.getTypeName()));
  private final Index index =
      SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
  private final AdapterToIndexMapping indexMapping =
      BaseDataStoreUtils.mapAdapterToIndex(internalAdapter, index);
  private final VisibilityHandler visibilityHandler = new UnconstrainedVisibilityHandler();
  private final AccumuloKeyValuePairGenerator<SimpleFeature> generator =
      new AccumuloKeyValuePairGenerator<>(internalAdapter, index, indexMapping, visibilityHandler);
  private SimpleFeature simpleFeature;
  private List<KeyValue> keyValuePairs;
  private final SimpleFeatureBuilder builder =
      new SimpleFeatureBuilder(GeonamesSimpleFeatureType.getInstance());
  private String[] geonamesEntryTokens;
  private String geonameId;
  private double longitude;
  private double latitude;
  private String location;

  @Override
  protected void map(final LongWritable key, final Text value, final Context context)
      throws IOException, InterruptedException {

    simpleFeature = parseGeonamesValue(value);

    // build Geowave-formatted Accumulo [Key,Value] pairs
    keyValuePairs = generator.constructKeyValuePairs(simpleFeature);

    // output each [Key,Value] pair to shuffle-and-sort phase where we rely
    // on MapReduce to sort by Key
    for (final KeyValue accumuloKeyValuePair : keyValuePairs) {
      context.write(accumuloKeyValuePair.getKey(), accumuloKeyValuePair.getValue());
    }
  }

  private SimpleFeature parseGeonamesValue(final Text value) {

    geonamesEntryTokens = value.toString().split("\\t"); // Exported Geonames entries are
    // tab-delimited

    geonameId = geonamesEntryTokens[0];
    location = geonamesEntryTokens[1];
    latitude = Double.parseDouble(geonamesEntryTokens[4]);
    longitude = Double.parseDouble(geonamesEntryTokens[5]);

    return buildSimpleFeature(geonameId, longitude, latitude, location);
  }

  private SimpleFeature buildSimpleFeature(
      final String featureId,
      final double longitude,
      final double latitude,
      final String location) {

    builder.set(
        "geometry",
        GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
    builder.set("Latitude", latitude);
    builder.set("Longitude", longitude);
    builder.set("Location", location);

    return builder.buildFeature(featureId);
  }
}
