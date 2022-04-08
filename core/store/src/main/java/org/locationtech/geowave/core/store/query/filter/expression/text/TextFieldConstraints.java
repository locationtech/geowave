/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.MultiDimensionalIndexData;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.text.BasicTextDataset;
import org.locationtech.geowave.core.index.text.TextData;
import org.locationtech.geowave.core.index.text.TextRange;
import org.locationtech.geowave.core.index.text.TextValue;
import org.locationtech.geowave.core.store.query.filter.expression.FilterRange;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Represents field constraints for text index data.
 */
public class TextFieldConstraints extends IndexFieldConstraints<String> {

  public TextFieldConstraints(
      final Map<Integer, DimensionConstraints<String>> dimensionConstraints) {
    super(dimensionConstraints);
  }

  /**
   * Converts the list of dimension constraints into multi-dimensional text data.
   * 
   * @param dimensionConstraints the list of dimension constraints
   * @return the index data from the constrained dimensions
   */
  public static List<MultiDimensionalIndexData<String>> toIndexData(
      final List<DimensionConstraints<String>> dimensionConstraints) {
    final List<MultiDimensionalIndexData<String>> results = Lists.newLinkedList();
    generateTextData(results, 0, dimensionConstraints, new TextData[0]);
    return results;
  }

  private static void generateTextData(
      final List<MultiDimensionalIndexData<String>> results,
      final int currentDimension,
      final List<DimensionConstraints<String>> dimensions,
      final TextData[] current) {
    if (currentDimension == dimensions.size()) {
      results.add(new BasicTextDataset(current));
      return;
    }
    final DimensionConstraints<String> dimension = dimensions.get(currentDimension);
    final List<FilterRange<String>> ranges = dimension.getRanges();
    for (int i = 0; i < ranges.size(); i++) {
      final TextData[] copy = Arrays.copyOf(current, current.length + 1);
      final TextFilterRange range = (TextFilterRange) ranges.get(i);
      final String start = toStartRangeValue(range.getStart());
      final String end = toEndRangeValue(range.getEnd());
      if (start.equals(end) && range.isStartInclusive() && range.isEndInclusive()) {
        copy[copy.length - 1] = new TextValue(start, range.isCaseSensitive(), range.isReversed());
      } else {
        copy[copy.length - 1] =
            new TextRange(
                toStartRangeValue(range.getStart()),
                toEndRangeValue(range.getEnd()),
                range.isStartInclusive(),
                range.isEndInclusive(),
                range.isCaseSensitive(),
                range.isReversed());
      }
      generateTextData(results, currentDimension + 1, dimensions, copy);
    }
  }

  private static String toStartRangeValue(final String value) {
    if (value == null) {
      return "";
    }
    return value;
  }

  private static String toEndRangeValue(final String value) {
    if (value == null) {
      return StringUtils.stringFromBinary(
          new byte[] {
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF});
    }
    return value;
  }

  /**
   * Create a set of text field constraints from the given filter ranges.
   * 
   * @param ranges the constrained ranges
   * @return the text field constraints
   */
  public static TextFieldConstraints of(final List<FilterRange<String>> ranges) {
    final Map<Integer, DimensionConstraints<String>> constraints = Maps.newHashMap();
    constraints.put(0, DimensionConstraints.of(ranges));
    return new TextFieldConstraints(constraints);
  }

  /**
   * Create a set of text field constraints from the given single range.
   * 
   * @param start the start of the range
   * @param end the end of the range
   * @param startInclusive whether or not the start of the range is inclusive
   * @param endInclusive whether or not the end of the range is inclusive
   * @param exact whether or not this range exactly represents the predicate
   * @param caseSensitive whether or not this range is case sensitive
   * @param reversed whether or not this range is for a reversed text index
   * @return the numeric field constraints
   */
  public static TextFieldConstraints of(
      final String start,
      final String end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact,
      final boolean caseSensitive,
      final boolean reversed) {
    return of(0, start, end, startInclusive, endInclusive, exact, caseSensitive, reversed);
  }

  /**
   * Create a set of text field constraints for a specific dimension from the given single range.
   * 
   * @param dimension the dimension for the constraints
   * @param start the start of the range
   * @param end the end of the range
   * @param startInclusive whether or not the start of the range is inclusive
   * @param endInclusive whether or not the end of the range is inclusive
   * @param exact whether or not this range exactly represents the predicate
   * @param caseSensitive whether or not this range is case sensitive
   * @param reversed whether or not this range is for a reversed text index
   * @return the numeric field constraints
   */
  public static TextFieldConstraints of(
      final Integer dimension,
      final String start,
      final String end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact,
      final boolean caseSensitive,
      final boolean reversed) {
    final Map<Integer, DimensionConstraints<String>> constraints = Maps.newHashMap();
    constraints.put(
        dimension,
        DimensionConstraints.of(
            Lists.newArrayList(
                TextFilterRange.of(
                    start,
                    end,
                    startInclusive,
                    endInclusive,
                    exact,
                    caseSensitive,
                    reversed))));
    return new TextFieldConstraints(constraints);
  }

}
