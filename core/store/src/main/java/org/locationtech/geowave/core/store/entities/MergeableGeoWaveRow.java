/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.entities;

import java.util.Arrays;

public abstract class MergeableGeoWaveRow implements GeoWaveRow {

  protected GeoWaveValue[] attributeValues;

  public MergeableGeoWaveRow() {}

  public MergeableGeoWaveRow(final GeoWaveValue[] attributeValues) {
    this.attributeValues = attributeValues;
  }

  @Override
  public final GeoWaveValue[] getFieldValues() {
    return attributeValues;
  }

  public void mergeRow(final MergeableGeoWaveRow row) {
    final GeoWaveValue[] rowFieldValues = row.getFieldValues();
    final GeoWaveValue[] newValues =
        Arrays.copyOf(attributeValues, attributeValues.length + rowFieldValues.length);
    System.arraycopy(rowFieldValues, 0, newValues, attributeValues.length, rowFieldValues.length);
    this.attributeValues = newValues;
    mergeRowInternal(row);
  }

  // In case any extending classes want to do something when rows are merged
  protected void mergeRowInternal(final MergeableGeoWaveRow row) {};

  public boolean shouldMerge(final GeoWaveRow row) {
    return (getAdapterId() == row.getAdapterId())
        && Arrays.equals(getDataId(), row.getDataId())
        && Arrays.equals(getPartitionKey(), row.getPartitionKey())
        && Arrays.equals(getSortKey(), row.getSortKey())
        && (getNumberOfDuplicates() == row.getNumberOfDuplicates());
  }
}
