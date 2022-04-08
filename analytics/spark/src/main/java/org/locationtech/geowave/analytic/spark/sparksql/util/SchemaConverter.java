/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.analytic.spark.sparksql.GeoWaveSpatialEncoders;
import org.locationtech.geowave.analytic.spark.sparksql.SimpleFeatureDataType;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConverter.class);

  public static SimpleFeatureType schemaToFeatureType(
      final StructType schema,
      final String typeName) {
    final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
    typeBuilder.setName(typeName);
    typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);
    try {
      typeBuilder.setCRS(CRS.decode("EPSG:4326", true));
    } catch (final FactoryException e) {
      LOGGER.error(e.getMessage(), e);
    }

    final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

    for (final StructField field : schema.fields()) {
      final AttributeDescriptor attrDesc = attrDescFromStructField(attrBuilder, field);

      typeBuilder.add(attrDesc);
    }

    return typeBuilder.buildFeatureType();
  }

  private static AttributeDescriptor attrDescFromStructField(
      final AttributeTypeBuilder attrBuilder,
      final StructField field) {
    if (field.name().equals("geom")) {
      return attrBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geom");
    }
    if (field.dataType() == DataTypes.StringType) {
      return attrBuilder.binding(String.class).buildDescriptor(field.name());
    } else if (field.dataType() == DataTypes.DoubleType) {
      return attrBuilder.binding(Double.class).buildDescriptor(field.name());
    } else if (field.dataType() == DataTypes.FloatType) {
      return attrBuilder.binding(Float.class).buildDescriptor(field.name());
    } else if (field.dataType() == DataTypes.LongType) {
      return attrBuilder.binding(Long.class).buildDescriptor(field.name());
    } else if (field.dataType() == DataTypes.IntegerType) {
      return attrBuilder.binding(Integer.class).buildDescriptor(field.name());
    } else if (field.dataType() == DataTypes.BooleanType) {
      return attrBuilder.binding(Boolean.class).buildDescriptor(field.name());
    } else if (field.dataType() == DataTypes.TimestampType) {
      return attrBuilder.binding(Date.class).buildDescriptor(field.name());
    }

    return null;
  }

  public static StructType schemaFromFeatureType(final SimpleFeatureType featureType) {
    final List<StructField> fields = new ArrayList<>();

    for (final AttributeDescriptor attrDesc : featureType.getAttributeDescriptors()) {
      final SimpleFeatureDataType sfDataType = attrDescToDataType(attrDesc);

      final String fieldName = (sfDataType.isGeom() ? "geom" : attrDesc.getName().getLocalPart());

      final StructField field =
          DataTypes.createStructField(fieldName, sfDataType.getDataType(), true);

      fields.add(field);
    }

    if (fields.isEmpty()) {
      LOGGER.error("Feature type produced empty dataframe schema!");
      return null;
    }

    return DataTypes.createStructType(fields);
  }

  private static SimpleFeatureDataType attrDescToDataType(final AttributeDescriptor attrDesc) {
    boolean isGeom = false;
    DataType dataTypeOut = DataTypes.NullType;

    if (attrDesc.getType().getBinding().equals(String.class)) {

      dataTypeOut = DataTypes.StringType;
    } else if (attrDesc.getType().getBinding().equals(Double.class)) {
      dataTypeOut = DataTypes.DoubleType;
    } else if (attrDesc.getType().getBinding().equals(Float.class)) {
      dataTypeOut = DataTypes.FloatType;
    } else if (attrDesc.getType().getBinding().equals(Long.class)) {
      dataTypeOut = DataTypes.LongType;
    } else if (attrDesc.getType().getBinding().equals(Integer.class)) {
      dataTypeOut = DataTypes.IntegerType;
    } else if (attrDesc.getType().getBinding().equals(Boolean.class)) {
      dataTypeOut = DataTypes.BooleanType;
    } else if (attrDesc.getType().getBinding().equals(Date.class)) {
      dataTypeOut = DataTypes.TimestampType;
    }

    // Custom geometry types get WKB encoding
    else if (Geometry.class.isAssignableFrom(attrDesc.getType().getBinding())) {
      dataTypeOut = GeoWaveSpatialEncoders.geometryUDT;
      isGeom = true;
    }

    return new SimpleFeatureDataType(dataTypeOut, isGeom);
  }
}
