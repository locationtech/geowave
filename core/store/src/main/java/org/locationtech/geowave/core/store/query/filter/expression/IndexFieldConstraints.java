/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Provides a set of multi-dimensional constraints for a single indexed field.
 */
public abstract class IndexFieldConstraints<V extends Comparable<V>> {
  protected final Map<Integer, DimensionConstraints<V>> dimensionConstraints;

  public IndexFieldConstraints(final Map<Integer, DimensionConstraints<V>> dimensionConstraints) {
    this.dimensionConstraints = dimensionConstraints;
  }

  /**
   * Get the constraints for the given dimension.
   * 
   * @param dimension the dimension to get constraints of
   * @return the dimension constraints, or {@code null} if there weren't any
   */
  public DimensionConstraints<V> getDimensionRanges(final int dimension) {
    return dimensionConstraints.get(dimension);
  }

  /**
   * @return the number of dimensions constrained for this field
   */
  public int getDimensionCount() {
    return dimensionConstraints.size();
  }

  /**
   * @return {@code true} if these constraints exactly represent the predicates for this field
   */
  public boolean isExact() {
    return dimensionConstraints.values().stream().allMatch(d -> d.isExact());
  }

  /**
   * Combine the constraints of this field with another set of constraints using an OR operator.
   * 
   * @param other the constraints to combine
   */
  public void or(final IndexFieldConstraints<V> other) {
    final Set<Integer> toRemove = Sets.newHashSet();
    for (final Entry<Integer, DimensionConstraints<V>> dimension : dimensionConstraints.entrySet()) {
      final DimensionConstraints<V> dimension1 = dimension.getValue();
      final DimensionConstraints<V> dimension2 = other.dimensionConstraints.get(dimension.getKey());
      if ((dimension1 == null) || (dimension2 == null)) {
        toRemove.add(dimension.getKey());
      } else {
        dimension1.or(dimension2);
      }
    }
    toRemove.stream().forEach(i -> dimensionConstraints.remove(i));
  }

  /**
   * Combine the constraints of this field with another set of constraints using an AND operator.
   * 
   * @param other the constraints to combine
   */
  public void and(final IndexFieldConstraints<V> other) {
    final Set<Integer> dimensions = Sets.newHashSet(dimensionConstraints.keySet());
    dimensions.addAll(other.dimensionConstraints.keySet());
    for (final Integer dimension : dimensions) {
      final DimensionConstraints<V> dimension1 = dimensionConstraints.get(dimension);
      final DimensionConstraints<V> dimension2 = other.dimensionConstraints.get(dimension);
      if (dimension1 == null) {
        dimensionConstraints.put(dimension, dimension2);
      } else if (dimension2 != null) {
        dimension1.and(dimension2);
      }
    }
  }

  /**
   * Invert the constraints of each dimension.
   */
  public void invert() {
    for (final Entry<Integer, DimensionConstraints<V>> dimension : dimensionConstraints.entrySet()) {
      dimension.getValue().invert();
    }
  }

  /**
   * A class representing the constraints of a single dimension of the field.
   *
   * @param <V> the constraint class
   */
  public static class DimensionConstraints<V extends Comparable<V>> {
    private List<FilterRange<V>> dimensionRanges = Lists.newArrayList();

    public DimensionConstraints(final List<FilterRange<V>> dimensionRanges) {
      this.dimensionRanges = dimensionRanges;
    }

    /**
     * @return a list of ranges that are constrained for this dimension
     */
    public List<FilterRange<V>> getRanges() {
      return dimensionRanges;
    }

    /**
     * @return {@code true} if this dimension constraints exactly represent the predicates for the
     *         dimension
     */
    public boolean isExact() {
      return dimensionRanges.stream().allMatch(r -> r.isExact());
    }

    /**
     * Combine the constraints of this dimension with another set of constraints using an OR
     * operator.
     * 
     * @param other the constraints to combine
     */
    public void or(final DimensionConstraints<V> other) {
      dimensionRanges.addAll(other.dimensionRanges);
      dimensionRanges = FilterRange.mergeRanges(dimensionRanges);
    }

    /**
     * Combine the constraints of this dimension with another set of constraints using an AND
     * operator.
     * 
     * @param other the constraints to combine
     */
    public void and(final DimensionConstraints<V> other) {
      dimensionRanges = FilterRange.intersectRanges(dimensionRanges, other.dimensionRanges);
    }

    /**
     * Invert the ranges of this dimension.
     */
    public void invert() {
      dimensionRanges = FilterRange.invertRanges(dimensionRanges);
    }

    /**
     * Create new dimension constraints from the specified set of filter ranges.
     * 
     * @param <V> the constraint class
     * @param ranges the constrained ranges
     * @return the constructed dimension constraints
     */
    public static <V extends Comparable<V>> DimensionConstraints<V> of(
        final List<FilterRange<V>> ranges) {
      return new DimensionConstraints<>(ranges);
    }
  }

}
