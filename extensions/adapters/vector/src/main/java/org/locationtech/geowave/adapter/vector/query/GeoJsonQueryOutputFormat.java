/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.locationtech.geowave.core.store.cli.query.QueryOutputFormatSpi;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Iterators;

public class GeoJsonQueryOutputFormat extends QueryOutputFormatSpi {
  public static final String FORMAT_NAME = "geojson";

  @Parameter(names = {"-o", "--outputFile"}, required = true, description = "Output file")
  private String outputFile;

  @Parameter(
      names = {"-t", "--typeName"},
      required = true,
      description = "Output feature type name")
  private String typeName = "results";

  public GeoJsonQueryOutputFormat() {
    super(FORMAT_NAME);
  }

  @Override
  public void output(ResultSet results) {
    int geometryColumn = -1;
    for (int i = 0; i < results.columnCount(); i++) {
      if (Geometry.class.isAssignableFrom(results.columnType(i))) {
        geometryColumn = i;
        break;
      }
    }
    if (geometryColumn < 0) {
      throw new RuntimeException(
          "Unable to output results to a geojson without a geometry column.");
    }

    SimpleFeatureTypeBuilder ftb = new SimpleFeatureTypeBuilder();
    ftb.setName(typeName);
    // TODO: This CRS needs to ultimately come from the query...
    // ftb.setCRS(results.getCRS());
    for (int i = 0; i < results.columnCount(); i++) {
      AttributeTypeBuilder atb = new AttributeTypeBuilder();
      atb.setBinding(results.columnType(i));
      atb.nillable(true);
      ftb.add(atb.buildDescriptor(results.columnName(i)));
    }
    SimpleFeatureType sft = ftb.buildFeatureType();
    final SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(sft);
    final AtomicLong nextId = new AtomicLong(0L);
    Iterator<SimpleFeature> features = Iterators.transform(results, r -> {
      sfb.reset();
      for (int i = 0; i < results.columnCount(); i++) {
        sfb.add(r.columnValue(i));
      }
      SimpleFeature feature = sfb.buildFeature(Long.toString(nextId.incrementAndGet()));
      return feature;
    });


    try {
      SimpleFeatureCollection featureCollection =
          DataUtilities.collection(new SimpleFeatureIterator() {
            @Override
            public boolean hasNext() {
              return features.hasNext();
            }

            @Override
            public SimpleFeature next() throws NoSuchElementException {
              return features.next();
            }

            @Override
            public void close() {}
          });
      FeatureJSON io = new FeatureJSON();
      io.writeFeatureCollection(featureCollection, outputFile);
    } catch (IOException e) {
      throw new RuntimeException(
          "Encountered exception when writing geojson file: " + e.getMessage(),
          e);
    }
  }

}
