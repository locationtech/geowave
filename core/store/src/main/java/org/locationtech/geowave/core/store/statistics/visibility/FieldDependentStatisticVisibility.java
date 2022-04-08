/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.visibility;

import java.util.Arrays;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import com.google.common.collect.Sets;

/**
 * Supplies visibility for a given field based on the bit position of that field in the index model.
 */
public class FieldDependentStatisticVisibility<T> implements EntryVisibilityHandler<T> {

  private final byte[] fieldBitmask;

  public FieldDependentStatisticVisibility(
      final CommonIndexModel model,
      final InternalDataAdapter<T> adapter,
      final String... fieldNames) {
    final SortedSet<Integer> bitPositions =
        Arrays.stream(fieldNames).map(
            field -> adapter.getPositionOfOrderedField(model, field)).collect(
                Collectors.toCollection(TreeSet::new));
    this.fieldBitmask = BitmaskUtils.generateCompositeBitmask(bitPositions);
  }

  @Override
  public byte[] getVisibility(final T entry, final GeoWaveRow... kvs) {
    if ((kvs.length == 1) && (kvs[0].getFieldValues().length == 1)) {
      return kvs[0].getFieldValues()[0].getVisibility();
    }
    final Set<String> visibilities = Sets.newHashSet();
    for (final GeoWaveRow r : kvs) {
      for (final GeoWaveValue v : r.getFieldValues()) {
        if ((v.getFieldMask() != null) && (v.getFieldMask().length > 0)) {
          if (BitmaskUtils.bitmaskOverlaps(v.getFieldMask(), fieldBitmask)) {
            visibilities.add(StringUtils.stringFromBinary(v.getVisibility()));
          }
        }
      }
    }
    if (visibilities.size() == 1) {
      return StringUtils.stringToBinary(visibilities.iterator().next());
    } else if (visibilities.size() > 1) {
      // This will combine all different visibilities using an AND operator. For example a
      // visibility of A and B will result in (A)&(B). Each token is wrapped in parentheses to
      // account for more complex visibility expressions.
      return StringUtils.stringToBinary(
          visibilities.stream().map(token -> "(" + token + ")").collect(Collectors.joining("&")));
    }
    return new byte[0];
  }
}
