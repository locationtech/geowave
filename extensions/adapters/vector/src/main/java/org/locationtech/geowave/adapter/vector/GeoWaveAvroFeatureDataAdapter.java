/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.visibility.VisibilityManagement;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/** */
public class GeoWaveAvroFeatureDataAdapter extends FeatureDataAdapter {

  protected GeoWaveAvroFeatureDataAdapter() {}

  public GeoWaveAvroFeatureDataAdapter(final SimpleFeatureType type) {
    super(
        type,
        new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>());
  }

  public GeoWaveAvroFeatureDataAdapter(
      final SimpleFeatureType type,
      final VisibilityManagement<SimpleFeature> visibilityManagement) {
    super(
        type,
        new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
        null,
        visibilityManagement);
  }

  public GeoWaveAvroFeatureDataAdapter(
      final SimpleFeatureType type,
      final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers) {
    super(type, customIndexHandlers);
  }

  public GeoWaveAvroFeatureDataAdapter(
      final SimpleFeatureType type,
      final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler) {
    super(type, fieldVisiblityHandler);
  }

  @Override
  protected List<NativeFieldHandler<SimpleFeature, Object>> getFieldHandlersFromFeatureType(
      final SimpleFeatureType type) {
    final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers = new ArrayList<>(1);

    nativeHandlers.add(new GeoWaveAvroFeatureAttributeHandler());
    return nativeHandlers;
  }

  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    if (fieldName.equals(GeoWaveAvroFeatureAttributeHandler.FIELD_NAME)) {
      return new GeoWaveAvroFeatureReader();
    }
    return super.getReader(fieldName);
  }

  @Override
  public FieldWriter<SimpleFeature, Object> getWriter(final String fieldName) {
    if (fieldName.equals(GeoWaveAvroFeatureAttributeHandler.FIELD_NAME)) {
      return new GeoWaveAvroFeatureWriter();
    }
    return super.getWriter(fieldName);
  }

  @Override
  public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {

    if (fieldName.equals(GeoWaveAvroFeatureAttributeHandler.FIELD_NAME)) {
      final List<String> dimensionFieldNames = getDimensionFieldNames(model);
      return dimensionFieldNames.size();
    }
    return super.getPositionOfOrderedField(model, fieldName);
  }

  @Override
  public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
    final List<String> dimensionFieldNames = getDimensionFieldNames(model);
    if (position < dimensionFieldNames.size()) {
      return dimensionFieldNames.get(position);
    } else if (position == dimensionFieldNames.size()) {
      return GeoWaveAvroFeatureAttributeHandler.FIELD_NAME;
    }
    return super.getFieldNameForPosition(model, position);
  }

  @Override
  protected RowBuilder<SimpleFeature, Object> newBuilder() {
    return new GeoWaveAvroAttributeRowBuilder();
  }
}
