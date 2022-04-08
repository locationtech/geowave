/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.core.store.cli.query.QueryOutputFormatSpi;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Iterators;

/**
 * Since most of this class is basic geotools data store logic, it would be easy to abstract out the
 * geotools portion and create several output formats for other geotools data store formats such as
 * postgis.
 */
public class ShapefileQueryOutputFormat extends QueryOutputFormatSpi {
  public static final String FORMAT_NAME = "shp";

  @Parameter(names = {"-o", "--outputFile"}, required = true, description = "Output file")
  private String outputFile;

  @Parameter(
      names = {"-t", "--typeName"},
      required = true,
      description = "Output feature type name")
  private String typeName = "results";

  public ShapefileQueryOutputFormat() {
    super(FORMAT_NAME);
  }

  @Override
  public void output(final ResultSet results) {
    int geometryColumn = -1;
    for (int i = 0; i < results.columnCount(); i++) {
      if (Geometry.class.isAssignableFrom(results.columnType(i))) {
        geometryColumn = i;
        break;
      }
    }
    if (geometryColumn < 0) {
      throw new RuntimeException(
          "Unable to output results to a shapefile without a geometry column.");
    }

    final SimpleFeatureTypeBuilder ftb = new SimpleFeatureTypeBuilder();
    // TODO: This CRS needs to ultimately come from the query...
    // ftb.setCRS(results.getCRS());
    ftb.setName(typeName);
    for (int i = 0; i < results.columnCount(); i++) {
      final AttributeTypeBuilder atb = new AttributeTypeBuilder();
      atb.setBinding(results.columnType(i));
      atb.nillable(true);
      if (i == geometryColumn) {
        ftb.add(atb.buildDescriptor("the_geom"));
      } else {
        ftb.add(atb.buildDescriptor(results.columnName(i)));
      }
    }
    final SimpleFeatureType sft = ftb.buildFeatureType();

    final SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(sft);
    final AtomicLong nextId = new AtomicLong(0L);
    final Iterator<SimpleFeature> features = Iterators.transform(results, r -> {
      sfb.reset();
      for (int i = 0; i < results.columnCount(); i++) {
        sfb.add(r.columnValue(i));
      }
      final SimpleFeature feature = sfb.buildFeature(Long.toString(nextId.incrementAndGet()));
      return feature;
    });

    final FileDataStoreFactorySpi factory = FileDataStoreFinder.getDataStoreFactory("shp");
    final File file = new File(outputFile);
    final Map<String, Serializable> params = Maps.newHashMap();
    final Transaction transaction = new DefaultTransaction("Write Results");
    try {
      params.put("url", file.toURI().toURL());
      final DataStore dataStore = factory.createNewDataStore(params);
      dataStore.createSchema(sft);
      final SimpleFeatureStore store =
          (SimpleFeatureStore) dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
      store.setTransaction(transaction);
      final SimpleFeatureCollection featureCollection =
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
      store.addFeatures(featureCollection);
      transaction.commit();
    } catch (final Exception e) {
      try {
        transaction.rollback();
      } catch (final IOException ioe) {
        throw new RuntimeException("Encountered an error when rolling back transaction", ioe);
      }
      throw new RuntimeException(
          "Encountered an error when writing the features to the file: " + e.getMessage(),
          e);
    }
  }

}
