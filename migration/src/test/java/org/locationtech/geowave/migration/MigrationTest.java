/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
import org.locationtech.geowave.core.geotime.adapter.TemporalFieldDescriptor;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.JsonFieldLevelVisibilityHandler;
import org.locationtech.geowave.migration.legacy.adapter.vector.LegacyFeatureDataAdapter;
import org.locationtech.geowave.migration.legacy.core.geotime.LegacyCustomCRSSpatialField;
import org.locationtech.geowave.migration.legacy.core.geotime.LegacyLatitudeField;
import org.locationtech.geowave.migration.legacy.core.geotime.LegacyLongitudeField;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;

public class MigrationTest {

  @Test
  public void testLegacyFeatureDataAdapterMigration() {
    final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
    builder.setName("testType");
    builder.setNamespaceURI("geowave.namespace");
    builder.add(
        attributeTypeBuilder.binding(String.class).nillable(true).buildDescriptor("strAttr"));
    builder.add(
        attributeTypeBuilder.binding(Integer.class).nillable(true).buildDescriptor("intAttr"));
    builder.add(
        attributeTypeBuilder.binding(Date.class).nillable(false).buildDescriptor("dateAttr"));
    builder.add(attributeTypeBuilder.binding(Point.class).nillable(false).buildDescriptor("geom"));
    builder.crs(GeometryUtils.getDefaultCRS());

    final SimpleFeatureType featureType = builder.buildFeatureType();
    final AttributeDescriptor stringAttr = featureType.getDescriptor("strAttr");
    // Configure legacy visiblity
    stringAttr.getUserData().put("visibility", Boolean.TRUE);
    featureType.getUserData().put(
        "visibilityManagerClass",
        "org.locationtech.geowave.adapter.vector.plugin.visibility.JsonDefinitionColumnVisibilityManagement");
    LegacyFeatureDataAdapter adapter = new LegacyFeatureDataAdapter(featureType, "EPSG:3257");

    final byte[] adapterBinary = PersistenceUtils.toBinary(adapter);
    final Persistable persistableAdapter = PersistenceUtils.fromBinary(adapterBinary);
    assertTrue(persistableAdapter instanceof LegacyFeatureDataAdapter);
    adapter = (LegacyFeatureDataAdapter) persistableAdapter;
    assertNotNull(adapter.getUpdatedAdapter());
    final FeatureDataAdapter updatedAdapter = adapter.getUpdatedAdapter();
    assertEquals(4, updatedAdapter.getFieldDescriptors().length);
    assertEquals(String.class, updatedAdapter.getFieldDescriptor("strAttr").bindingClass());
    assertEquals(Integer.class, updatedAdapter.getFieldDescriptor("intAttr").bindingClass());
    assertTrue(
        TemporalFieldDescriptor.class.isAssignableFrom(
            updatedAdapter.getFieldDescriptor("dateAttr").getClass()));
    final TemporalFieldDescriptor<?> temporalField =
        (TemporalFieldDescriptor<?>) updatedAdapter.getFieldDescriptor("dateAttr");
    assertEquals(Date.class, temporalField.bindingClass());
    assertTrue(temporalField.indexHints().contains(TimeField.TIME_DIMENSION_HINT));
    assertTrue(
        SpatialFieldDescriptor.class.isAssignableFrom(
            updatedAdapter.getFieldDescriptor("geom").getClass()));
    final SpatialFieldDescriptor<?> spatialField =
        (SpatialFieldDescriptor<?>) updatedAdapter.getFieldDescriptor("geom");
    assertEquals(Point.class, spatialField.bindingClass());
    assertEquals(GeometryUtils.getDefaultCRS(), spatialField.crs());
    assertTrue(spatialField.indexHints().contains(SpatialField.LATITUDE_DIMENSION_HINT));
    assertTrue(spatialField.indexHints().contains(SpatialField.LONGITUDE_DIMENSION_HINT));
    assertEquals("testType", updatedAdapter.getTypeName());
    assertEquals(SimpleFeature.class, updatedAdapter.getDataClass());
    assertTrue(updatedAdapter.hasTemporalConstraints());
    assertNotNull(adapter.getVisibilityHandler());
    final VisibilityHandler visibilityHandler = adapter.getVisibilityHandler();
    assertTrue(visibilityHandler instanceof JsonFieldLevelVisibilityHandler);
    assertEquals(
        "strAttr",
        ((JsonFieldLevelVisibilityHandler) visibilityHandler).getVisibilityAttribute());
  }

  @Test
  public void testLegacySpatialFields() throws NoSuchAuthorityCodeException, FactoryException {

    LegacyLatitudeField latitudeFullRange = new LegacyLatitudeField(4, false);
    byte[] fieldBytes = PersistenceUtils.toBinary(latitudeFullRange);
    latitudeFullRange = (LegacyLatitudeField) PersistenceUtils.fromBinary(fieldBytes);
    LatitudeField updatedLatitudeField = latitudeFullRange.getUpdatedField(null);
    assertEquals(GeometryUtils.getDefaultCRS(), updatedLatitudeField.getCRS());
    assertEquals(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, updatedLatitudeField.getFieldName());
    assertEquals(180, updatedLatitudeField.getRange(), 0.0001);
    assertEquals(-90, updatedLatitudeField.getFullRange().getMin(), 0.0001);
    assertEquals(90, updatedLatitudeField.getFullRange().getMax(), 0.0001);
    assertEquals(4, (int) updatedLatitudeField.getGeometryPrecision());

    LegacyLatitudeField latitudeHalfRange = new LegacyLatitudeField(4, true);
    fieldBytes = PersistenceUtils.toBinary(latitudeHalfRange);
    latitudeHalfRange = (LegacyLatitudeField) PersistenceUtils.fromBinary(fieldBytes);
    updatedLatitudeField = latitudeHalfRange.getUpdatedField(null);
    assertEquals(GeometryUtils.getDefaultCRS(), updatedLatitudeField.getCRS());
    assertEquals(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, updatedLatitudeField.getFieldName());
    assertEquals(360, updatedLatitudeField.getRange(), 0.0001);
    assertEquals(-180, updatedLatitudeField.getFullRange().getMin(), 0.0001);
    assertEquals(180, updatedLatitudeField.getFullRange().getMax(), 0.0001);
    assertEquals(4, (int) updatedLatitudeField.getGeometryPrecision());

    LegacyLongitudeField longitudeField = new LegacyLongitudeField(4);
    fieldBytes = PersistenceUtils.toBinary(longitudeField);
    longitudeField = (LegacyLongitudeField) PersistenceUtils.fromBinary(fieldBytes);
    final LongitudeField updatedLongitudeField = longitudeField.getUpdatedField(null);
    assertEquals(GeometryUtils.getDefaultCRS(), updatedLongitudeField.getCRS());
    assertEquals(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, updatedLongitudeField.getFieldName());
    assertEquals(360, updatedLongitudeField.getRange(), 0.0001);
    assertEquals(-180, updatedLongitudeField.getFullRange().getMin(), 0.0001);
    assertEquals(180, updatedLongitudeField.getFullRange().getMax(), 0.0001);
    assertEquals(4, (int) updatedLongitudeField.getGeometryPrecision());

    final SpatialOptions options = new SpatialOptions();
    options.setCrs("EPSG:3257");
    options.setGeometryPrecision(4);
    final CoordinateReferenceSystem crs = CRS.decode("EPSG:3257", true);
    final Index index = SpatialDimensionalityTypeProvider.createIndexFromOptions(options);
    for (int i = 0; i < crs.getCoordinateSystem().getDimension(); i++) {
      final CoordinateSystemAxis csa = crs.getCoordinateSystem().getAxis(i);
      LegacyCustomCRSSpatialField customCRSField;
      if (i == 0) {
        customCRSField =
            new LegacyCustomCRSSpatialField(
                new CustomCRSBoundedSpatialDimensionX(csa.getMinimumValue(), csa.getMaximumValue()),
                4);
      } else {
        customCRSField =
            new LegacyCustomCRSSpatialField(
                new CustomCRSBoundedSpatialDimensionY(csa.getMinimumValue(), csa.getMaximumValue()),
                4);
      }

      fieldBytes = PersistenceUtils.toBinary(customCRSField);
      customCRSField = (LegacyCustomCRSSpatialField) PersistenceUtils.fromBinary(fieldBytes);
      final CustomCRSSpatialField updatedCRSField = customCRSField.getUpdatedField(index);
      assertEquals(crs, updatedCRSField.getCRS());
      assertEquals(SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, updatedCRSField.getFieldName());
      assertEquals(
          csa.getMaximumValue() - csa.getMinimumValue(),
          updatedCRSField.getRange(),
          0.0001);
      assertEquals(csa.getMinimumValue(), updatedCRSField.getFullRange().getMin(), 0.0001);
      assertEquals(csa.getMaximumValue(), updatedCRSField.getFullRange().getMax(), 0.0001);
      assertEquals(4, (int) updatedCRSField.getGeometryPrecision());
      if (i == 0) {
        assertTrue(
            CustomCRSBoundedSpatialDimensionX.class.isAssignableFrom(
                updatedCRSField.getBaseDefinition().getClass()));
      } else {
        assertTrue(
            CustomCRSBoundedSpatialDimensionY.class.isAssignableFrom(
                updatedCRSField.getBaseDefinition().getClass()));
      }
    }

  }

}
