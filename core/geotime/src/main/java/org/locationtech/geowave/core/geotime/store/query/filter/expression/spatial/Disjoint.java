/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import java.nio.ByteBuffer;
import java.util.Set;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;

/**
 * Predicate that passes when the first operand is disjoint from the second operand.
 */
public class Disjoint extends BinarySpatialPredicate {

  private boolean loose;

  public Disjoint() {}

  public Disjoint(
      final SpatialExpression expression1,
      final SpatialExpression expression2,
      final boolean loose) {
    super(expression1, expression2);
    this.loose = loose;
  }

  @Override
  public boolean evaluateInternal(final FilterGeometry value1, final FilterGeometry value2) {
    if (loose) {
      return value1.getGeometry().getEnvelopeInternal().disjoint(
          value2.getGeometry().getEnvelopeInternal());
    }
    return value1.disjoint(value2);
  }

  @Override
  public <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    // This is a full scan because there isn't currently a way to do a set of constraints with a
    // hole in it.
    return FilterConstraints.empty();
  }

  @Override
  protected boolean isExact() {
    return isLoose();
  }

  public boolean isLoose() {
    return loose;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(loose ? "LOOSE_DISJOINT(" : "DISJOINT(");
    sb.append(expression1.toString());
    sb.append(",");
    sb.append(expression2.toString());
    sb.append(")");
    return sb.toString();
  }

  @Override
  public byte[] toBinary() {
    final byte[] superBinary = super.toBinary();
    final ByteBuffer buffer = ByteBuffer.allocate(1 + superBinary.length);
    buffer.put(loose ? (byte) 1 : (byte) 0);
    buffer.put(superBinary);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    loose = buffer.get() != 0;
    final byte[] superBinary = new byte[buffer.remaining()];
    buffer.get(superBinary);
    super.fromBinary(superBinary);
  }
}
