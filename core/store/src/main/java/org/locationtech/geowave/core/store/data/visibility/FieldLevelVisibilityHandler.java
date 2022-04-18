/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.VisibilityHandler;

/**
 * Determines the visibility of a field based on the value in another field in the entry.
 */
public class FieldLevelVisibilityHandler implements VisibilityHandler {

  private String visibilityAttribute;

  public FieldLevelVisibilityHandler() {}

  public FieldLevelVisibilityHandler(final String visibilityAttribute) {
    super();
    this.visibilityAttribute = visibilityAttribute;
  }

  public String getVisibilityAttribute() {
    return visibilityAttribute;
  }

  /**
   * Determine the visibility of the given field based on the value of the visibility field.
   *
   * @param visibilityObject the value of the visibility field
   * @param fieldName the field to determine the visibility of
   * @return the visibility of the field
   */
  protected String translateVisibility(final Object visibilityObject, final String fieldName) {
    if (visibilityObject == null) {
      return null;
    }
    return visibilityObject.toString();
  }

  @Override
  public <T> String getVisibility(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final String fieldName) {

    final Object visibilityAttributeValue = adapter.getFieldValue(entry, visibilityAttribute);
    return translateVisibility(visibilityAttributeValue, fieldName);
  }

  @Override
  public byte[] toBinary() {
    return StringUtils.stringToBinary(visibilityAttribute);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    visibilityAttribute = StringUtils.stringFromBinary(bytes);
  }
}
