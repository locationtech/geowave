/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Abstract implementation for comparing two expressions that evaluate to comparable objects.
 *
 * @param <E> the expression class
 * @param <C> the comparable class
 */
public abstract class ComparisonOperator<E extends Expression<C>, C extends Comparable<C>> extends
    BinaryPredicate<E> {

  public enum CompareOp {
    LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, EQUAL_TO, NOT_EQUAL_TO
  }

  protected CompareOp compareOperator;

  public ComparisonOperator() {}

  public ComparisonOperator(
      final E expression1,
      final E expression2,
      final CompareOp compareOperator) {
    super(expression1, expression2);
    this.compareOperator = compareOperator;
  }

  public CompareOp getCompareOp() {
    return compareOperator;
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final C value1 = expression1.evaluateValue(fieldValues);
    final C value2 = expression2.evaluateValue(fieldValues);
    return evaluateValues(value1, value2);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final C value1 = expression1.evaluateValue(adapter, entry);
    final C value2 = expression2.evaluateValue(adapter, entry);
    return evaluateValues(value1, value2);
  }

  private boolean evaluateValues(final C value1, final C value2) {
    if (value1 == null) {
      if (compareOperator.equals(CompareOp.EQUAL_TO)) {
        return value2 == null;
      }
      if (compareOperator.equals(CompareOp.NOT_EQUAL_TO)) {
        return value2 != null;
      }
      return false;
    }
    if (value2 == null) {
      if (compareOperator.equals(CompareOp.EQUAL_TO)) {
        return false;
      }
      if (compareOperator.equals(CompareOp.NOT_EQUAL_TO)) {
        return true;
      }
      return false;
    }
    switch (compareOperator) {
      case EQUAL_TO:
        return equalTo(value1, value2);
      case NOT_EQUAL_TO:
        return notEqualTo(value1, value2);
      case LESS_THAN:
        return lessThan(value1, value2);
      case LESS_THAN_OR_EQUAL:
        return lessThanOrEqual(value1, value2);
      case GREATER_THAN:
        return greaterThan(value1, value2);
      case GREATER_THAN_OR_EQUAL:
        return greaterThanOrEqual(value1, value2);
    }
    return false;
  }

  protected abstract boolean equalTo(final C value1, final C value2);

  protected abstract boolean notEqualTo(final C value1, final C value2);

  protected abstract boolean lessThan(final C value1, final C value2);

  protected abstract boolean lessThanOrEqual(final C value1, final C value2);

  protected abstract boolean greaterThan(final C value1, final C value2);

  protected abstract boolean greaterThanOrEqual(final C value1, final C value2);

  protected abstract boolean indexSupported(final Index index);

  protected FilterRange<C> toFilterRange(
      final C start,
      final C end,
      final boolean startInclusve,
      final boolean endInclusive) {
    return FilterRange.of(start, end, startInclusve, endInclusive, isExact());
  }

  protected boolean isExact() {
    return true;
  }

  protected abstract IndexFieldConstraints<C> toFieldConstraints(final List<FilterRange<C>> ranges);

  @Override
  public Set<String> getConstrainableFields() {
    if ((expression1 instanceof FieldValue) && expression2.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) expression1).getFieldName());
    } else if ((expression2 instanceof FieldValue) && expression1.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) expression2).getFieldName());
    }
    return Sets.newHashSet();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    final List<FilterRange<C>> ranges = Lists.newArrayList();
    if (!indexSupported(index)) {
      return FilterConstraints.empty();
    }
    C literal = null;
    String fieldName = null;
    CompareOp compareOp = this.compareOperator;
    if ((expression1 instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) expression1).getFieldName())
        && expression2.isLiteral()) {
      literal = expression2.evaluateValue(null, null);
      fieldName = ((FieldValue<?>) expression1).getFieldName();
    } else if ((expression2 instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) expression2).getFieldName())
        && expression1.isLiteral()) {
      literal = expression1.evaluateValue(null, null);
      fieldName = ((FieldValue<?>) expression2).getFieldName();
      switch (compareOperator) {
        case LESS_THAN:
          compareOp = CompareOp.GREATER_THAN;
          break;
        case LESS_THAN_OR_EQUAL:
          compareOp = CompareOp.GREATER_THAN_OR_EQUAL;
          break;
        case GREATER_THAN:
          compareOp = CompareOp.LESS_THAN;
          break;
        case GREATER_THAN_OR_EQUAL:
          compareOp = CompareOp.LESS_THAN_OR_EQUAL;
          break;
        default:
          break;
      }
    } else {
      return FilterConstraints.empty();
    }
    if (literal != null) {
      if (!constraintClass.isAssignableFrom(literal.getClass())) {
        return FilterConstraints.empty();
      }
      switch (compareOp) {
        case LESS_THAN:
          ranges.add(toFilterRange(null, literal, true, false));
          break;
        case LESS_THAN_OR_EQUAL:
          ranges.add(toFilterRange(null, literal, true, true));
          break;
        case GREATER_THAN:
          ranges.add(toFilterRange(literal, null, false, true));
          break;
        case GREATER_THAN_OR_EQUAL:
          ranges.add(toFilterRange(literal, null, true, true));
          break;
        case EQUAL_TO:
          ranges.add(toFilterRange(literal, literal, true, true));
          break;
        case NOT_EQUAL_TO:
          ranges.add(toFilterRange(null, literal, true, false));
          ranges.add(toFilterRange(literal, null, false, true));
          break;
      }
    }
    return FilterConstraints.of(
        adapter,
        indexMapping,
        index,
        fieldName,
        (IndexFieldConstraints<V>) toFieldConstraints(ranges));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(expression1.toString());
    sb.append(' ');
    switch (compareOperator) {
      case LESS_THAN:
        sb.append("<");
        break;
      case LESS_THAN_OR_EQUAL:
        sb.append("<=");
        break;
      case GREATER_THAN:
        sb.append(">");
        break;
      case GREATER_THAN_OR_EQUAL:
        sb.append(">=");
        break;
      case EQUAL_TO:
        sb.append("=");
        break;
      case NOT_EQUAL_TO:
        sb.append("<>");
        break;
    }
    sb.append(' ');
    sb.append(expression2.toString());
    return sb.toString();
  }

  @Override
  public byte[] toBinary() {
    final byte[] superBinary = super.toBinary();
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(compareOperator.ordinal()) + superBinary.length);
    VarintUtils.writeUnsignedInt(compareOperator.ordinal(), buffer);
    buffer.put(superBinary);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    compareOperator = CompareOp.values()[VarintUtils.readUnsignedInt(buffer)];
    final byte[] superBinary = new byte[buffer.remaining()];
    buffer.get(superBinary);
    super.fromBinary(superBinary);
  }
}
