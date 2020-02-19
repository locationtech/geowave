/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.adapter.IndexFieldHandler;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

public class FeatureAttributeCommonIndexFieldHandler implements
    IndexFieldHandler<SimpleFeature, FeatureAttributeCommonIndexValue, Object> {
  protected final AttributeDescriptor attrDesc;
  private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;

  public FeatureAttributeCommonIndexFieldHandler(final AttributeDescriptor attrDesc) {
    this.attrDesc = attrDesc;
    visibilityHandler = null;
  }

  public FeatureAttributeCommonIndexFieldHandler(
      final AttributeDescriptor attrDesc,
      final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler) {
    super();
    this.attrDesc = attrDesc;
    this.visibilityHandler = visibilityHandler;
  }

  @Override
  public String[] getNativeFieldNames() {
    return new String[] {attrDesc.getLocalName()};
  }

  @Override
  public FeatureAttributeCommonIndexValue toIndexValue(final SimpleFeature row) {
    final Object value = row.getAttribute(attrDesc.getName());
    if (value == null) {
      return null;
    }
    Number number;
    if (value instanceof Number) {
      number = (Number) value;
    } else if (TimeUtils.isTemporal(value.getClass())) {
      number = TimeUtils.getTimeMillis(value);
    } else {
      return null;
    }
    byte[] visibility;
    if (visibilityHandler != null) {
      visibility = visibilityHandler.getVisibility(row, attrDesc.getLocalName(), value);
    } else {
      visibility = new byte[] {};
    }
    return new FeatureAttributeCommonIndexValue(number, visibility);
  }

  @Override
  public FeatureAttributeCommonIndexValue toIndexValue(
      final PersistentDataset<Object> adapterPersistenceEncoding) {
    // visibility is unnecessary because this only happens after the number is read (its only used
    // in reconstructing common index values when using a secondary index)
    final Object obj = adapterPersistenceEncoding.getValue(attrDesc.getLocalName());
    final Number number;
    if (obj instanceof Number) {
      number = (Number) obj;
    } else if (TimeUtils.isTemporal(obj.getClass())) {
      number = TimeUtils.getTimeMillis(obj);
    } else {
      return null;
    }
    return new FeatureAttributeCommonIndexValue(number, null);
  }

  @Override
  public PersistentValue<Object>[] toNativeValues(
      final FeatureAttributeCommonIndexValue indexValue) {
    return new PersistentValue[] {
        new PersistentValue<>(attrDesc.getLocalName(), indexValue.getValue())};
  }

}
