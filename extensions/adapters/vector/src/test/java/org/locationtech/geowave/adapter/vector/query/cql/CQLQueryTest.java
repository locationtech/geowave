/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ExplicitCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class CQLQueryTest {
  private static final Index SPATIAL_INDEX =
      SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
  private static final Index SPATIAL_TEMPORAL_INDEX =
      SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
          new SpatialTemporalOptions());
  SimpleFeatureType type;
  InternalGeotoolsFeatureDataAdapter<SimpleFeature> adapter;

  @Before
  public void init() throws SchemaException {
    type =
        DataUtilities.createType(
            "geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,pid:String");
    final FeatureDataAdapter a = new FeatureDataAdapter(type);
    adapter = (InternalGeotoolsFeatureDataAdapter<SimpleFeature>) a.asInternalAdapter((short) -1);
  }

  @Test
  public void testGeoAndTemporalWithMatchingIndex() throws CQLException {
    final ExplicitCQLQuery query =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
            adapter,
            null,
            null);
    final List<MultiDimensionalNumericData> constraints =
        query.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(
        Arrays.equals(
            constraints.get(0).getMinValuesPerDimension(),
            new Double[] {27.2, 41.2, 1.116534776001E12}));
    assertTrue(
        Arrays.equals(
            constraints.get(0).getMaxValuesPerDimension(),
            new Double[] {27.3, 41.3, 1.116538375999E12}));
  }

  @Test
  public void testGeoAndTemporalWithNonMatchingIndex() throws CQLException {
    final ExplicitCQLQuery query =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
            adapter,
            null,
            null);
    final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(SPATIAL_INDEX);
    assertTrue(
        Arrays.equals(constraints.get(0).getMinValuesPerDimension(), new Double[] {27.2, 41.2}));
    assertTrue(
        Arrays.equals(constraints.get(0).getMaxValuesPerDimension(), new Double[] {27.3, 41.3}));
  }

  @Test
  public void testGeoWithMatchingIndex() throws CQLException {
    final ExplicitCQLQuery query =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20)",
            adapter,
            null,
            null);
    final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(SPATIAL_INDEX);
    assertTrue(
        Arrays.equals(constraints.get(0).getMinValuesPerDimension(), new Double[] {27.2, 41.2}));
    assertTrue(
        Arrays.equals(constraints.get(0).getMaxValuesPerDimension(), new Double[] {27.3, 41.3}));
  }

  @Test
  public void testNoConstraintsWithGeoIndex() throws CQLException {
    final ExplicitCQLQuery query =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery("pid = '10'", adapter, null, null);
    assertTrue(query.getIndexConstraints(SPATIAL_INDEX).isEmpty());
  }

  @Test
  public void testNoConstraintsWithTemporalIndex() throws CQLException {
    final ExplicitCQLQuery query =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery("pid = '10'", adapter, null, null);
    assertTrue(query.getIndexConstraints(SPATIAL_TEMPORAL_INDEX).isEmpty());
  }

  @Test
  public void testGeoWithTemporalIndex() throws CQLException {
    final ExplicitCQLQuery query =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20)",
            adapter,
            null,
            null);
    assertTrue(query.getIndexConstraints(SPATIAL_TEMPORAL_INDEX).isEmpty());
  }

  @Test
  public void testGeoTemporalRangeWithMatchingIndex() throws CQLException, SchemaException {
    final SimpleFeatureType type =
        DataUtilities.createType(
            "geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");
    final FeatureDataAdapter a = new FeatureDataAdapter(type);
    final InternalGeotoolsFeatureDataAdapter<SimpleFeature> adapter =
        (InternalGeotoolsFeatureDataAdapter<SimpleFeature>) a.asInternalAdapter((short) -1);
    final ExplicitCQLQuery query =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20) and start during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
            adapter,
            null,
            null);
    final List<MultiDimensionalNumericData> constraints =
        query.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(
        Arrays.equals(
            constraints.get(0).getMinValuesPerDimension(),
            new Double[] {27.2, 41.2, 1.116534776001E12}));
    assertTrue(
        Arrays.equals(
            constraints.get(0).getMaxValuesPerDimension(),
            new Double[] {27.3, 41.3, 1.116538375999E12}));
    final ExplicitCQLQuery query2 =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20) and end during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
            adapter,
            null,
            null);
    final List<MultiDimensionalNumericData> constraints2 =
        query2.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(
        Arrays.equals(
            constraints2.get(0).getMinValuesPerDimension(),
            new Double[] {27.2, 41.2, 1.116534776001E12}));
    assertTrue(
        Arrays.equals(
            constraints2.get(0).getMaxValuesPerDimension(),
            new Double[] {27.3, 41.3, 1.116538375999E12}));

    final ExplicitCQLQuery query3 =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20) and (start before 2005-05-19T21:32:56Z and end after 2005-05-19T20:32:56Z)",
            adapter,
            null,
            null);
    final List<MultiDimensionalNumericData> constraints3 =
        query3.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(
        Arrays.equals(
            constraints3.get(0).getMinValuesPerDimension(),
            new Double[] {27.2, 41.2, 1.116534776001E12}));
    assertTrue(
        Arrays.equals(
            constraints3.get(0).getMaxValuesPerDimension(),
            new Double[] {27.3, 41.3, 1.116538375999E12}));

    final ExplicitCQLQuery query4 =
        (ExplicitCQLQuery) OptimalCQLQuery.createOptimalQuery(
            "BBOX(geometry,27.20,41.30,27.30,41.20) and (start after 2005-05-19T20:32:56Z and end after 2005-05-19T20:32:56Z)",
            adapter,
            null,
            null);
    final List<MultiDimensionalNumericData> constraints4 =
        query4.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(
        Arrays.equals(
            constraints4.get(0).getMinValuesPerDimension(),
            new Double[] {27.2, 41.2, 1.116534776001E12}));
    assertTrue(
        Arrays.equals(
            constraints4.get(0).getMaxValuesPerDimension(),
            new Double[] {27.3, 41.3, 9.223372036854775999E18}));
  }
}
