/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.util.DateUtilities;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class FeatureDataAdapterTest {

  private SimpleFeatureType schema;
  private SimpleFeature newFeature;
  private Date time1;
  private Date time2;

  GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws SchemaException, CQLException, ParseException {

    time1 = DateUtilities.parseISO("2005-05-19T18:33:55Z");
    time2 = DateUtilities.parseISO("2005-05-19T19:33:55Z");

    schema =
        DataUtilities.createType(
            "sp.geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String");

    newFeature =
        FeatureDataUtils.buildFeature(
            schema,
            new Pair[] {
                Pair.of("geometry", factory.createPoint(new Coordinate(27.25, 41.25))),
                Pair.of("pop", Long.valueOf(100)),
                Pair.of("when", time1),
                Pair.of("whennot", time2)});
  }

  @Test
  public void testDifferentProjection() throws SchemaException {
    final SimpleFeatureType schema =
        DataUtilities.createType("sp.geostuff", "geometry:Geometry:srid=3005,pop:java.lang.Long");
    final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(schema);
    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(
            dataAdapter.asInternalAdapter((short) -1),
            spatialIndex);
    final CoordinateReferenceSystem crs =
        dataAdapter.getFeatureType().getCoordinateReferenceSystem();
    // assertTrue(crs.getIdentifiers().toString().contains("EPSG:4326"));

    @SuppressWarnings("unchecked")
    final SimpleFeature newFeature =
        FeatureDataUtils.buildFeature(
            schema,
            new Pair[] {
                Pair.of("geometry", factory.createPoint(new Coordinate(27.25, 41.25))),
                Pair.of("pop", Long.valueOf(100))});
    final AdapterPersistenceEncoding persistenceEncoding =
        dataAdapter.asInternalAdapter((short) -1).encode(newFeature, indexMapping, spatialIndex);

    Geometry geom = null;
    for (final Entry<String, ?> pv : persistenceEncoding.getCommonData().getValues().entrySet()) {
      if (pv.getValue() instanceof Geometry) {
        geom = (Geometry) pv.getValue();
      }
    }
    assertNotNull(geom);

    assertEquals(new Coordinate(-138.0, 44.0), geom.getCentroid().getCoordinate());
  }

  @Test
  public void testSingleTime() {
    schema.getDescriptor("when").getUserData().clear();
    schema.getDescriptor("whennot").getUserData().put("time", Boolean.TRUE);

    final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(schema);
    final Index spatialIndex =
        SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
            new SpatialTemporalOptions());
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(
            dataAdapter.asInternalAdapter((short) -1),
            spatialIndex);
    final byte[] binary = dataAdapter.toBinary();

    final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
    dataAdapterCopy.fromBinary(binary);

    assertEquals(dataAdapterCopy.getTypeName(), dataAdapter.getTypeName());
    assertEquals(dataAdapterCopy.getFeatureType(), dataAdapter.getFeatureType());
    assertEquals(
        Boolean.TRUE,
        dataAdapterCopy.getFeatureType().getDescriptor("whennot").getUserData().get("time"));

    assertEquals(2, indexMapping.getIndexFieldMappers().size());
    assertNotNull(indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID));
    assertEquals(
        1,
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).adapterFieldCount());
    assertEquals(
        "whennot",
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).getAdapterFields()[0]);
    assertNotNull(indexMapping.getMapperForIndexField(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME));
    assertEquals(
        1,
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).adapterFieldCount());
    assertEquals(
        "geometry",
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).getAdapterFields()[0]);
  }

  @Test
  public void testInferredTime() {

    schema.getDescriptor("when").getUserData().clear();
    schema.getDescriptor("whennot").getUserData().clear();

    final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(schema);
    final Index spatialIndex =
        SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
            new SpatialTemporalOptions());
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(
            dataAdapter.asInternalAdapter((short) -1),
            spatialIndex);
    final byte[] binary = dataAdapter.toBinary();

    final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
    dataAdapterCopy.fromBinary(binary);

    assertEquals(dataAdapterCopy.getTypeName(), dataAdapter.getTypeName());
    assertEquals(dataAdapterCopy.getFeatureType(), dataAdapter.getFeatureType());
    assertEquals(
        Boolean.TRUE,
        dataAdapterCopy.getFeatureType().getDescriptor("when").getUserData().get("time"));

    assertEquals(2, indexMapping.getIndexFieldMappers().size());
    assertNotNull(indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID));
    assertEquals(
        1,
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).adapterFieldCount());
    assertEquals(
        "when",
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).getAdapterFields()[0]);
    assertNotNull(indexMapping.getMapperForIndexField(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME));
    assertEquals(
        1,
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).adapterFieldCount());
    assertEquals(
        "geometry",
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).getAdapterFields()[0]);
  }

  @Test
  public void testRange() {

    schema.getDescriptor("when").getUserData().clear();
    schema.getDescriptor("whennot").getUserData().clear();

    schema.getDescriptor("when").getUserData().put("start", Boolean.TRUE);
    schema.getDescriptor("whennot").getUserData().put("end", Boolean.TRUE);

    final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(schema);
    final Index spatialIndex =
        SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
            new SpatialTemporalOptions());
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(
            dataAdapter.asInternalAdapter((short) -1),
            spatialIndex);
    final byte[] binary = dataAdapter.toBinary();

    final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
    dataAdapterCopy.fromBinary(binary);

    assertEquals(dataAdapterCopy.getTypeName(), dataAdapter.getTypeName());
    assertEquals(dataAdapterCopy.getFeatureType(), dataAdapter.getFeatureType());
    assertEquals(
        Boolean.TRUE,
        dataAdapterCopy.getFeatureType().getDescriptor("whennot").getUserData().get("end"));
    assertEquals(
        Boolean.TRUE,
        dataAdapterCopy.getFeatureType().getDescriptor("when").getUserData().get("start"));


    assertEquals(2, indexMapping.getIndexFieldMappers().size());
    assertNotNull(indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID));
    assertEquals(
        2,
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).adapterFieldCount());
    assertEquals(
        "when",
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).getAdapterFields()[0]);
    assertEquals(
        "whennot",
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).getAdapterFields()[1]);
    assertNotNull(indexMapping.getMapperForIndexField(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME));
    assertEquals(
        1,
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).adapterFieldCount());
    assertEquals(
        "geometry",
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).getAdapterFields()[0]);
  }

  @Test
  public void testInferredRange() throws SchemaException {

    final SimpleFeatureType schema =
        DataUtilities.createType(
            "http://foo",
            "sp.geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");

    final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
    final Object[] defaults = new Object[descriptors.size()];
    int p = 0;
    for (final AttributeDescriptor descriptor : descriptors) {
      defaults[p++] = descriptor.getDefaultValue();
    }

    final SimpleFeature newFeature =
        SimpleFeatureBuilder.build(schema, defaults, UUID.randomUUID().toString());

    newFeature.setAttribute("pop", Long.valueOf(100));
    newFeature.setAttribute("pid", UUID.randomUUID().toString());
    newFeature.setAttribute("start", time1);
    newFeature.setAttribute("end", time2);
    newFeature.setAttribute("geometry", factory.createPoint(new Coordinate(27.25, 41.25)));

    final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(schema);
    final Index spatialIndex =
        SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
            new SpatialTemporalOptions());
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(
            dataAdapter.asInternalAdapter((short) -1),
            spatialIndex);
    final byte[] binary = dataAdapter.toBinary();

    final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
    dataAdapterCopy.fromBinary(binary);

    assertEquals("http://foo", dataAdapterCopy.getFeatureType().getName().getNamespaceURI());

    assertEquals(dataAdapterCopy.getTypeName(), dataAdapter.getTypeName());
    assertEquals(dataAdapterCopy.getFeatureType(), dataAdapter.getFeatureType());
    assertEquals(
        Boolean.TRUE,
        dataAdapterCopy.getFeatureType().getDescriptor("end").getUserData().get("end"));
    assertEquals(
        Boolean.TRUE,
        dataAdapterCopy.getFeatureType().getDescriptor("start").getUserData().get("start"));

    assertEquals(2, indexMapping.getIndexFieldMappers().size());
    assertNotNull(indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID));
    assertEquals(
        2,
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).adapterFieldCount());
    assertEquals(
        "start",
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).getAdapterFields()[0]);
    assertEquals(
        "end",
        indexMapping.getMapperForIndexField(TimeField.DEFAULT_FIELD_ID).getAdapterFields()[1]);
    assertNotNull(indexMapping.getMapperForIndexField(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME));
    assertEquals(
        1,
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).adapterFieldCount());
    assertEquals(
        "geometry",
        indexMapping.getMapperForIndexField(
            SpatialField.DEFAULT_GEOMETRY_FIELD_NAME).getAdapterFields()[0]);
  }

  @Test
  public void testCRSProjection() {
    final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
    typeBuilder.setName("test");
    typeBuilder.setCRS(GeometryUtils.getDefaultCRS()); // <- Coordinate
    // reference
    // add attributes in order
    typeBuilder.add("geom", Point.class);
    typeBuilder.add("name", String.class);
    typeBuilder.add("count", Long.class);

    // build the type
    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(typeBuilder.buildFeatureType());

    final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(builder.getFeatureType());
    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final byte[] binary = dataAdapter.toBinary();

    final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
    dataAdapterCopy.fromBinary(binary);

    assertEquals(
        dataAdapterCopy.getFeatureType().getCoordinateReferenceSystem().getCoordinateSystem(),
        GeometryUtils.getDefaultCRS().getCoordinateSystem());
  }
}
