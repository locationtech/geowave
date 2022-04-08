/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class SpatialTemporalAnnotationsTest {

  private static final String TEST_CRS_CODE = "EPSG:3857";

  @Test
  public void testSpatialTemporalAnnotations()
      throws NoSuchAuthorityCodeException, FactoryException {
    BasicDataTypeAdapter<TestType> adapter =
        BasicDataTypeAdapter.newAdapter("myType", TestType.class, "name");

    final CoordinateReferenceSystem testCRS = CRS.decode(TEST_CRS_CODE);
    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(3, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("name"));
    assertTrue(String.class.isAssignableFrom(adapter.getFieldDescriptor("name").bindingClass()));
    SpatialFieldDescriptor<?> geometryDescriptor =
        (SpatialFieldDescriptor<?>) adapter.getFieldDescriptor("geometry");
    assertNotNull(geometryDescriptor);
    assertTrue(Geometry.class.isAssignableFrom(geometryDescriptor.bindingClass()));
    assertTrue(geometryDescriptor.indexHints().contains(SpatialField.LATITUDE_DIMENSION_HINT));
    assertTrue(geometryDescriptor.indexHints().contains(SpatialField.LONGITUDE_DIMENSION_HINT));
    assertEquals(testCRS, geometryDescriptor.crs());
    assertNotNull(adapter.getFieldDescriptor("date"));
    assertTrue(Date.class.isAssignableFrom(adapter.getFieldDescriptor("date").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("date").indexHints().contains(TimeField.TIME_DIMENSION_HINT));

    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    adapter = (BasicDataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);

    assertEquals("myType", adapter.getTypeName());
    assertEquals(TestType.class, adapter.getDataClass());
    assertEquals(3, adapter.getFieldDescriptors().length);
    assertNotNull(adapter.getFieldDescriptor("name"));
    assertTrue(String.class.isAssignableFrom(adapter.getFieldDescriptor("name").bindingClass()));
    geometryDescriptor = (SpatialFieldDescriptor<?>) adapter.getFieldDescriptor("geometry");
    assertTrue(Geometry.class.isAssignableFrom(geometryDescriptor.bindingClass()));
    assertTrue(geometryDescriptor.indexHints().contains(SpatialField.LATITUDE_DIMENSION_HINT));
    assertTrue(geometryDescriptor.indexHints().contains(SpatialField.LONGITUDE_DIMENSION_HINT));
    assertEquals(testCRS, geometryDescriptor.crs());
    assertNotNull(adapter.getFieldDescriptor("date"));
    assertTrue(Date.class.isAssignableFrom(adapter.getFieldDescriptor("date").bindingClass()));
    assertTrue(
        adapter.getFieldDescriptor("date").indexHints().contains(TimeField.TIME_DIMENSION_HINT));

    final TestType testEntry =
        new TestType(
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(5, 5)),
            new Date(100),
            "id1");
    assertEquals("id1", adapter.getFieldValue(testEntry, "name"));
    assertTrue(
        GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(5, 5)).equalsExact(
            (Geometry) adapter.getFieldValue(testEntry, "geometry")));
    assertEquals(new Date(100), adapter.getFieldValue(testEntry, "date"));

    final Object[] fields = new Object[3];
    for (int i = 0; i < fields.length; i++) {
      switch (adapter.getFieldDescriptors()[i].fieldName()) {
        case "name":
          fields[i] = "id1";
          break;
        case "geometry":
          fields[i] = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 10));
          break;
        case "date":
          fields[i] = new Date(500);
          break;
      }
    }

    final TestType builtEntry = adapter.buildObject("id1", fields);
    assertEquals("id1", adapter.getFieldValue(builtEntry, "name"));
    assertTrue(
        GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 10)).equalsExact(
            (Geometry) adapter.getFieldValue(builtEntry, "geometry")));
    assertEquals(new Date(500), adapter.getFieldValue(builtEntry, "date"));
  }

  @GeoWaveDataType
  private static class TestType {

    @GeoWaveSpatialField(crs = TEST_CRS_CODE, spatialIndexHint = true)
    private Geometry geometry;

    @GeoWaveTemporalField(timeIndexHint = true)
    private Date date;

    @GeoWaveField
    private String name;

    protected TestType() {}

    public TestType(final Geometry geometry, final Date date, final String name) {
      this.geometry = geometry;
      this.date = date;
      this.name = name;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    public Date getDate() {
      return date;
    }

    public String getName() {
      return name;
    }
  }
}
