/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import java.util.Date;
import java.util.List;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveSpatialField;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveTemporalField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.aggregate.AbstractAggregationTest;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import com.google.common.collect.Lists;

public class AbstractVectorAggregationTest extends AbstractAggregationTest {
  protected static final String ID_COLUMN = "id";
  protected static final String GEOMETRY_COLUMN = "geometry";
  protected static final String TIMESTAMP_COLUMN = "timestamp";
  protected static final String LATITUDE_COLUMN = "latitude";
  protected static final String LONGITUDE_COLUMN = "longitude";
  protected static final String VALUE_COLUMN = "value";
  protected static final String ODDS_NULL_COLUMN = "oddsNull";
  protected static final String ALL_NULL_COLUMN = "allNull";

  protected DataTypeAdapter<SpatialTestType> adapter =
      BasicDataTypeAdapter.newAdapter("testType", SpatialTestType.class, "id");

  public static SpatialTestType createFeature(
      final int featureId,
      final int longitude,
      final int latitude) {
    return new SpatialTestType(
        String.valueOf(featureId),
        GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)),
        new Date(),
        latitude,
        longitude,
        featureId,
        featureId % 2 == 0 ? "NotNull" : null,
        null);
  }

  public static List<SpatialTestType> generateFeatures() {

    final List<SpatialTestType> features = Lists.newArrayList();

    int featureId = 0;
    for (int longitude = -180; longitude <= 180; longitude += 1) {
      for (int latitude = -90; latitude <= 90; latitude += 1) {

        features.add(createFeature(featureId, longitude, latitude));
        featureId++;
      }
    }
    return features;
  }

  @GeoWaveDataType
  protected static class SpatialTestType {
    @GeoWaveField
    private String id;

    @GeoWaveSpatialField
    private Point geometry;

    @GeoWaveTemporalField
    private Date timestamp;

    @GeoWaveField
    private double latitude;

    @GeoWaveField
    private double longitude;

    @GeoWaveField
    private long value;

    @GeoWaveField
    private String oddsNull;

    @GeoWaveField
    private String allNull;

    public SpatialTestType() {}

    public SpatialTestType(
        final String id,
        final Point geometry,
        final Date timestamp,
        final double latitude,
        final double longitude,
        final long value,
        final String oddsNull,
        final String allNull) {
      this.id = id;
      this.geometry = geometry;
      this.timestamp = timestamp;
      this.latitude = latitude;
      this.longitude = longitude;
      this.value = value;
      this.oddsNull = oddsNull;
      this.allNull = allNull;
    }
  }

}
