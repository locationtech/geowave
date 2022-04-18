/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

/**
 * Provides a hint on an adapter field to tell GeoWave that the field should be used for a
 * particular type of index field.
 */
public class IndexDimensionHint {

  private final String hint;

  public IndexDimensionHint(final String hint) {
    this.hint = hint;
  }

  public String getHintString() {
    return hint;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof IndexDimensionHint)) {
      return false;
    }
    return hint.equals(((IndexDimensionHint) obj).hint);
  }

  @Override
  public int hashCode() {
    return hint.hashCode();
  }
}
