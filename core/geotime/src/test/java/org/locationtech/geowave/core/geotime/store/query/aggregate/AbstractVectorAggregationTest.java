/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import java.util.Date;
import java.util.List;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.aggregate.AbstractAggregationTest;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import com.google.common.collect.Lists;

public class AbstractVectorAggregationTest<P extends Persistable, R> extends
    AbstractAggregationTest<P, R, SimpleFeature> {

  protected static final String GEOMETRY_COLUMN = "geometry";
  protected static final String TIMESTAMP_COLUMN = "TimeStamp";
  protected static final String LATITUDE_COLUMN = "Latitude";
  protected static final String LONGITUDE_COLUMN = "Longitude";
  protected static final String VALUE_COLUMN = "Value";
  protected static final String ODDS_NULL_COLUMN = "OddsNull";
  protected static final String ALL_NULL_COLUMN = "AllNull";

  public List<SimpleFeature> generateFeatures() {
    final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder ab = new AttributeTypeBuilder();

    typeBuilder.setName("features");
    typeBuilder.add(ab.binding(Geometry.class).nillable(false).buildDescriptor(GEOMETRY_COLUMN));
    typeBuilder.add(ab.binding(Date.class).nillable(true).buildDescriptor(TIMESTAMP_COLUMN));
    typeBuilder.add(ab.binding(Double.class).nillable(false).buildDescriptor(LATITUDE_COLUMN));
    typeBuilder.add(ab.binding(Double.class).nillable(false).buildDescriptor(LONGITUDE_COLUMN));
    typeBuilder.add(ab.binding(Long.class).nillable(false).buildDescriptor(VALUE_COLUMN));
    typeBuilder.add(ab.binding(String.class).nillable(true).buildDescriptor(ODDS_NULL_COLUMN));
    typeBuilder.add(ab.binding(String.class).nillable(true).buildDescriptor(ALL_NULL_COLUMN));

    List<SimpleFeature> features = Lists.newArrayList();
    final SimpleFeatureBuilder featureBuilder =
        new SimpleFeatureBuilder(typeBuilder.buildFeatureType());

    int featureId = 0;
    for (int longitude = -180; longitude <= 180; longitude += 1) {
      for (int latitude = -90; latitude <= 90; latitude += 1) {
        featureBuilder.set(
            GEOMETRY_COLUMN,
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
        featureBuilder.set(TIMESTAMP_COLUMN, new Date());
        featureBuilder.set(LATITUDE_COLUMN, latitude);
        featureBuilder.set(LONGITUDE_COLUMN, longitude);
        featureBuilder.set(VALUE_COLUMN, (long) featureId);
        if (featureId % 2 == 0) {
          featureBuilder.set(ODDS_NULL_COLUMN, "NotNull");
        }

        features.add(featureBuilder.buildFeature(String.valueOf(featureId)));
        featureId++;
      }
    }
    return features;
  }

}
