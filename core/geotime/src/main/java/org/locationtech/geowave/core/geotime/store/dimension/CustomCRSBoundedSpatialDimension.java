/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;

public class CustomCRSBoundedSpatialDimension extends BasicDimensionDefinition implements
    CustomCRSSpatialDimension {
  private BaseCustomCRSSpatialDimension baseCustomCRS;

  public CustomCRSBoundedSpatialDimension() {}

  public CustomCRSBoundedSpatialDimension(final byte axis, final double min, final double max) {
    super(min, max);
    baseCustomCRS = new BaseCustomCRSSpatialDimension(axis);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result) + ((baseCustomCRS == null) ? 0 : baseCustomCRS.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CustomCRSBoundedSpatialDimension other = (CustomCRSBoundedSpatialDimension) obj;
    if (baseCustomCRS == null) {
      if (other.baseCustomCRS != null) {
        return false;
      }
    } else if (!baseCustomCRS.equals(other.baseCustomCRS)) {
      return false;
    }
    return true;
  }

  @Override
  public byte[] toBinary() {

    // TODO future issue to investigate performance improvements associated
    // with excessive array/object allocations
    // serialize axis
    return baseCustomCRS.addAxisToBinary(super.toBinary());
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    // TODO future issue to investigate performance improvements associated
    // with excessive array/object allocations
    // deserialize axis
    baseCustomCRS = new BaseCustomCRSSpatialDimension();
    super.fromBinary(baseCustomCRS.getAxisFromBinaryAndRemove(bytes));
  }

  @Override
  public byte getAxis() {
    return baseCustomCRS.getAxis();
  }
}
