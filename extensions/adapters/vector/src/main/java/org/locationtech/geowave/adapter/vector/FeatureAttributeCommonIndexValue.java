/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

public class FeatureAttributeCommonIndexValue implements CommonIndexValue {
  private final Number value;
  private byte[] visibility;

  public FeatureAttributeCommonIndexValue(final Number value, final byte[] visibility) {
    this.value = value;
    this.visibility = visibility;
  }

  public Number getValue() {
    return value;
  }

  @Override
  public byte[] getVisibility() {
    return visibility;
  }

  @Override
  public void setVisibility(final byte[] visibility) {
    this.visibility = visibility;
  }

  @Override
  public boolean overlaps(final NumericDimensionField[] field, final NumericData[] rangeData) {
    return (value != null)
        && (value.doubleValue() <= rangeData[0].getMax())
        && (value.doubleValue() >= rangeData[0].getMin());
  }
}
