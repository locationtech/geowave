/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import static org.junit.Assert.assertEquals;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.adapter.LatLonFieldMapper.DoubleLatLonFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.LatLonFieldMapper.FloatLatLonFieldMapper;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField.SpatialIndexFieldOptions;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import com.google.common.collect.Lists;

public class SpatialFieldMapperTest {

  @Test
  public void testGeometryFieldMapper() throws FactoryException {
    FieldDescriptor<Geometry> testField =
        new SpatialFieldDescriptorBuilder<>(Geometry.class).crs(
            CRS.decode("EPSG:3857")).spatialIndexHint().fieldName("testField").build();
    GeometryFieldMapper mapper = new GeometryFieldMapper();
    mapper.init(
        "idx",
        Lists.newArrayList(testField),
        new SpatialIndexFieldOptions(CRS.decode("EPSG:4326")));

    assertEquals(Geometry.class, mapper.indexFieldType());
    assertEquals(Geometry.class, mapper.adapterFieldType());
    assertEquals(1, mapper.adapterFieldCount());
    assertEquals("testField", mapper.getAdapterFields()[0]);

    final byte[] mapperBinary = PersistenceUtils.toBinary(mapper);
    mapper = (GeometryFieldMapper) PersistenceUtils.fromBinary(mapperBinary);

    assertEquals(Geometry.class, mapper.indexFieldType());
    assertEquals(Geometry.class, mapper.adapterFieldType());
    assertEquals(1, mapper.adapterFieldCount());
    assertEquals("testField", mapper.getAdapterFields()[0]);
  }

  @Test
  public void testDoubleLatLonFieldMapper() throws FactoryException {
    FieldDescriptor<Double> latitude =
        new SpatialFieldDescriptorBuilder<>(Double.class).crs(
            CRS.decode("EPSG:3857")).latitudeIndexHint().fieldName("lat").build();
    FieldDescriptor<Double> longitude =
        new SpatialFieldDescriptorBuilder<>(Double.class).crs(
            CRS.decode("EPSG:3857")).longitudeIndexHint().fieldName("lon").build();
    DoubleLatLonFieldMapper mapper = new DoubleLatLonFieldMapper();
    mapper.init(
        "idx",
        Lists.newArrayList(latitude, longitude),
        new SpatialIndexFieldOptions(CRS.decode("EPSG:4326")));

    assertEquals(Geometry.class, mapper.indexFieldType());
    assertEquals(Double.class, mapper.adapterFieldType());
    assertEquals(2, mapper.adapterFieldCount());
    assertEquals("lat", mapper.getAdapterFields()[0]);
    assertEquals("lon", mapper.getAdapterFields()[1]);
    assertEquals(false, mapper.xAxisFirst);

    final byte[] mapperBinary = PersistenceUtils.toBinary(mapper);
    mapper = (DoubleLatLonFieldMapper) PersistenceUtils.fromBinary(mapperBinary);

    assertEquals(Geometry.class, mapper.indexFieldType());
    assertEquals(Double.class, mapper.adapterFieldType());
    assertEquals(2, mapper.adapterFieldCount());
    assertEquals("lat", mapper.getAdapterFields()[0]);
    assertEquals("lon", mapper.getAdapterFields()[1]);
    assertEquals(false, mapper.xAxisFirst);
  }

  @Test
  public void testFloatLatLonFieldMapper() throws FactoryException {
    FieldDescriptor<Float> longitude =
        new SpatialFieldDescriptorBuilder<>(Float.class).crs(
            CRS.decode("EPSG:3857")).longitudeIndexHint().fieldName("lon").build();
    FieldDescriptor<Float> latitude =
        new SpatialFieldDescriptorBuilder<>(Float.class).crs(
            CRS.decode("EPSG:3857")).latitudeIndexHint().fieldName("lat").build();
    FloatLatLonFieldMapper mapper = new FloatLatLonFieldMapper();
    mapper.init(
        "idx",
        Lists.newArrayList(longitude, latitude),
        new SpatialIndexFieldOptions(CRS.decode("EPSG:4326")));

    assertEquals(Geometry.class, mapper.indexFieldType());
    assertEquals(Float.class, mapper.adapterFieldType());
    assertEquals(2, mapper.adapterFieldCount());
    assertEquals("lon", mapper.getAdapterFields()[0]);
    assertEquals("lat", mapper.getAdapterFields()[1]);
    assertEquals(true, mapper.xAxisFirst);

    final byte[] mapperBinary = PersistenceUtils.toBinary(mapper);
    mapper = (FloatLatLonFieldMapper) PersistenceUtils.fromBinary(mapperBinary);

    assertEquals(Geometry.class, mapper.indexFieldType());
    assertEquals(Float.class, mapper.adapterFieldType());
    assertEquals(2, mapper.adapterFieldCount());
    assertEquals("lon", mapper.getAdapterFields()[0]);
    assertEquals("lat", mapper.getAdapterFields()[1]);
    assertEquals(true, mapper.xAxisFirst);
  }

}
