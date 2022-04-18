/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.MultiDimensionalIndexData;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.numeric.NumericValue;
import org.locationtech.geowave.core.store.query.filter.expression.FilterRange;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Represents field constraints for numeric index data.
 */
public class NumericFieldConstraints extends IndexFieldConstraints<Double> {

  public NumericFieldConstraints(
      final Map<Integer, DimensionConstraints<Double>> dimensionConstraints) {
    super(dimensionConstraints);
  }

  /**
   * Converts the list of dimension constraints into multi-dimensional numeric data.
   * 
   * @param dimensionConstraints the list of dimension constraints
   * @return the index data from the constrained dimensions
   */
  public static List<MultiDimensionalIndexData<Double>> toIndexData(
      final List<DimensionConstraints<Double>> dimensionConstraints) {
    final List<MultiDimensionalIndexData<Double>> results = Lists.newLinkedList();
    generateNumericData(results, 0, dimensionConstraints, new NumericData[0]);
    return results;
  }

  private static void generateNumericData(
      final List<MultiDimensionalIndexData<Double>> results,
      final int currentDimension,
      final List<DimensionConstraints<Double>> dimensions,
      final NumericData[] current) {
    if (currentDimension == dimensions.size()) {
      results.add(new BasicNumericDataset(current));
      return;
    }
    final DimensionConstraints<Double> dimension = dimensions.get(currentDimension);
    final List<FilterRange<Double>> ranges = dimension.getRanges();
    for (int i = 0; i < ranges.size(); i++) {
      final NumericData[] copy = Arrays.copyOf(current, current.length + 1);
      final FilterRange<Double> range = ranges.get(i);
      final Double start = toStartRangeValue(range.getStart());
      final Double end = toEndRangeValue(range.getEnd());
      if (start.equals(end) && range.isStartInclusive() && range.isEndInclusive()) {
        copy[copy.length - 1] = new NumericValue(start);
      } else {
        copy[copy.length - 1] =
            new NumericRange(
                toStartRangeValue(range.getStart()),
                toEndRangeValue(range.getEnd()),
                range.isStartInclusive(),
                range.isEndInclusive());
      }
      generateNumericData(results, currentDimension + 1, dimensions, copy);
    }
  }

  private static double toStartRangeValue(final Double value) {
    if (value == null) {
      return Double.NEGATIVE_INFINITY;
    }
    return value;
  }

  private static double toEndRangeValue(final Double value) {
    if (value == null) {
      return Double.POSITIVE_INFINITY;
    }
    return value;
  }

  /**
   * Create a set of numeric field constraints from the given filter ranges.
   * 
   * @param ranges the constrained ranges
   * @return the numeric field constraints
   */
  public static NumericFieldConstraints of(final List<FilterRange<Double>> ranges) {
    final Map<Integer, DimensionConstraints<Double>> constraints = Maps.newHashMap();
    constraints.put(0, DimensionConstraints.of(ranges));
    return new NumericFieldConstraints(constraints);
  }

  /**
   * Create a set of numeric field constraints from the given dimension constraints.
   * 
   * @param dimensionConstraints a map of constraints for each dimension
   * @return the numeric field constraints
   */
  public static NumericFieldConstraints of(
      final Map<Integer, DimensionConstraints<Double>> dimensionConstraints) {
    return new NumericFieldConstraints(dimensionConstraints);
  }

  /**
   * Create a set of numeric field constraints from the given single range.
   * 
   * @param start the start of the range
   * @param end the end of the range
   * @param startInclusive whether or not the start of the range is inclusive
   * @param endInclusive whether or not the end of the range is inclusive
   * @param exact whether or not this range exactly represents the predicate
   * @return the numeric field constraints
   */
  public static NumericFieldConstraints of(
      final Double start,
      final Double end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact) {
    return of(0, start, end, startInclusive, endInclusive, exact);
  }

  /**
   * Create a set of numeric field constraints from the given single range for a dimension.
   * 
   * @param dimension the dimension that this range is on
   * @param start the start of the range
   * @param end the end of the range
   * @param startInclusive whether or not the start of the range is inclusive
   * @param endInclusive whether or not the end of the range is inclusive
   * @param exact whether or not this range exactly represents the predicate
   * @return the numeric field constraints
   */
  public static NumericFieldConstraints of(
      final Integer dimension,
      final Double start,
      final Double end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact) {
    final Map<Integer, DimensionConstraints<Double>> constraints = Maps.newHashMap();
    constraints.put(
        dimension,
        DimensionConstraints.of(
            Lists.newArrayList(FilterRange.of(start, end, startInclusive, endInclusive, exact))));
    return new NumericFieldConstraints(constraints);
  }

}
