/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import com.google.common.collect.Lists;

/**
 * A range of data represented by a predicate.
 * 
 * @param <T> the class of the filtered data
 */
public class FilterRange<T extends Comparable<T>> implements Comparable<FilterRange<T>> {

  private final T start;
  private final T end;

  private boolean startInclusive = true;
  private boolean endInclusive = true;

  private boolean exact = false;

  /**
   * Create a new filter range with the given parameters. A {@code null} start indicates an open
   * ended start, while a {@code null} end indicates an open ended end.
   * 
   * @param start the start of the range
   * @param end the end of the range
   * @param startInclusive whether or not the start is inclusive
   * @param endInclusive whether or not the end is inclusive
   * @param exact whether or not this range exactly represents the predicate
   */
  public FilterRange(
      final @Nullable T start,
      final @Nullable T end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact) {
    this.start = start;
    this.end = end;
    this.startInclusive = startInclusive;
    this.endInclusive = endInclusive;
    this.exact = exact;
  }

  public T getStart() {
    return start;
  }

  public T getEnd() {
    return end;
  }

  public boolean isStartInclusive() {
    return startInclusive;
  }

  public boolean isEndInclusive() {
    return endInclusive;
  }

  /**
   * @return {@code true} if this range exactly represents the predicate
   */
  public boolean isExact() {
    return exact;
  }

  /**
   * @return {@code true} if this range represents all data
   */
  public boolean isFullRange() {
    return (start == null) && (end == null) && startInclusive && endInclusive;
  }

  protected boolean isAfter(final FilterRange<T> other, final boolean startPoint) {
    if (getStart() == null) {
      return false;
    }
    final T point = startPoint ? other.start : other.end;
    if (point == null) {
      return startPoint;
    }
    return start.compareTo(point) > 0;
  }

  protected boolean isBefore(final FilterRange<T> other, final boolean startPoint) {
    if (getEnd() == null) {
      return false;
    }
    final T point = startPoint ? other.start : other.end;
    if (point == null) {
      return !startPoint;
    }
    return end.compareTo(point) < 0;
  }

  protected boolean overlaps(final FilterRange<T> other) {
    return !isAfter(other, false) && !isBefore(other, true);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + (start == null ? -1 : start.hashCode());
    result = (prime * result) + (end == null ? -1 : end.hashCode());
    result = (prime * result) + (startInclusive ? 1 : 0);
    result = (prime * result) + (endInclusive ? 1 : 0);
    return result;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof FilterRange)) {
      return false;
    }
    final FilterRange<?> otherRange = (FilterRange<?>) other;
    final boolean startMatches =
        start == null ? otherRange.start == null : start.equals(otherRange.start);
    final boolean endMatches = end == null ? otherRange.end == null : end.equals(otherRange.end);
    return startMatches
        && endMatches
        && (startInclusive == otherRange.startInclusive)
        && (endInclusive == otherRange.endInclusive);
  }

  @Override
  public int compareTo(final FilterRange<T> o) {
    int compare;
    if (getStart() == null) {
      if (o.getStart() == null) {
        compare = 0;
      } else {
        compare = -1;
      }
    } else if (o.getStart() == null) {
      compare = 1;
    } else {
      compare = getStart().compareTo(o.getStart());
      if (compare == 0) {
        compare = Boolean.compare(o.startInclusive, startInclusive);
      }
    }
    if (compare == 0) {
      if (getEnd() == null) {
        if (o.getEnd() == null) {
          compare = 0;
        } else {
          compare = -1;
        }
      } else if (o.getEnd() == null) {
        compare = 1;
      } else {
        compare = getEnd().compareTo(o.getEnd());
        if (compare == 0) {
          compare = Boolean.compare(o.endInclusive, endInclusive);
        }
      }
    }
    return compare;
  }

  private FilterRange<T> intersectRange(final FilterRange<T> other) {
    final T intersectStart;
    final T intersectEnd;
    final boolean startInc;
    final boolean endInc;
    if (start == null) {
      if (other.start == null) {
        intersectStart = null;
        startInc = startInclusive && other.startInclusive;
      } else {
        intersectStart = other.start;
        startInc = other.startInclusive;
      }
    } else if (other.start == null) {
      intersectStart = start;
      startInc = startInclusive;
    } else {
      final int compare = start.compareTo(other.start);
      if (compare > 0) {
        intersectStart = start;
        startInc = startInclusive;
      } else if (compare == 0) {
        intersectStart = start;
        startInc = startInclusive && other.startInclusive;
      } else {
        intersectStart = other.start;
        startInc = other.startInclusive;
      }
    }
    if (end == null) {
      if (other.end == null) {
        intersectEnd = null;
        endInc = endInclusive && other.endInclusive;
      } else {
        intersectEnd = other.end;
        endInc = other.endInclusive;
      }
    } else if (other.end == null) {
      intersectEnd = end;
      endInc = endInclusive;
    } else {
      final int compare = end.compareTo(other.end);
      if (compare < 0) {
        intersectEnd = end;
        endInc = endInclusive;
      } else if (compare == 0) {
        intersectEnd = end;
        endInc = endInclusive && other.endInclusive;
      } else {
        intersectEnd = other.end;
        endInc = other.endInclusive;
      }
    }
    return FilterRange.of(intersectStart, intersectEnd, startInc, endInc, exact && other.exact);
  }

  /**
   * Create a new filter range with the given parameters. A {@code null} start indicates an open
   * ended start, while a {@code null} end indicates an open ended end.
   * 
   * @param T the class of the filter range
   * @param start the start of the range
   * @param end the end of the range
   * @param startInclusive whether or not the start is inclusive
   * @param endInclusive whether or not the end is inclusive
   * @param exact whether or not this range exactly represents the predicate
   * @return the filter range
   */
  public static <T extends Comparable<T>> FilterRange<T> of(
      final T start,
      final T end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact) {
    return new FilterRange<>(start, end, startInclusive, endInclusive, exact);
  }

  /**
   * Creates a new filter range that represents all data.
   * 
   * @param <T> the class of the filter range
   * @return the filter range
   */
  public static <T extends Comparable<T>> FilterRange<T> include() {
    return FilterRange.of(null, null, true, true, true);
  }


  /**
   * Merges a list of filter ranges into their most simple form. Overlapping ranges will be merged
   * together.
   * 
   * @param <T> the class of the filter range
   * @param ranges the ranges to merge
   * @return the merged ranges
   */
  public static <T extends Comparable<T>> List<FilterRange<T>> mergeRanges(
      final List<FilterRange<T>> ranges) {
    if (ranges.size() <= 1) {
      return ranges;
    }
    Collections.sort(ranges);
    final List<FilterRange<T>> mergedRanges = Lists.newArrayList();
    FilterRange<T> currentRange = null;
    for (final FilterRange<T> range : ranges) {
      if (currentRange == null) {
        currentRange = range;
        continue;
      }
      if (currentRange.isBefore(range, true)) {
        mergedRanges.add(currentRange);
        currentRange = range;
      }
      if (currentRange.isBefore(range, false)) {
        currentRange =
            FilterRange.of(
                currentRange.start,
                range.end,
                currentRange.startInclusive,
                range.endInclusive,
                currentRange.exact && range.exact);
      }
    }
    if (currentRange != null) {
      mergedRanges.add(currentRange);
    }
    return mergedRanges;
  }

  /**
   * Intersects a list of filter ranges with another list of filter ranges. It is assumed that both
   * lists represent merged (non-overlapping) data.
   * 
   * @param <T> the class of the filter range
   * @param ranges1 the first set of ranges
   * @param ranges2 the second set of ranges
   * @return a list of filter ranges that represents the data that is represented by both lists
   */
  public static <T extends Comparable<T>> List<FilterRange<T>> intersectRanges(
      final List<FilterRange<T>> ranges1,
      final List<FilterRange<T>> ranges2) {
    Collections.sort(ranges1);
    Collections.sort(ranges2);
    final List<FilterRange<T>> intersections = Lists.newLinkedList();
    int i = 0, j = 0;
    while ((i < ranges1.size()) && (j < ranges2.size())) {
      final FilterRange<T> range1 = ranges1.get(i);
      final FilterRange<T> range2 = ranges2.get(j);
      if (range1.isBefore(range2, false)) {
        i++;
      } else {
        j++;
      }

      if (range1.overlaps(range2)) {
        intersections.add(range1.intersectRange(range2));
      }
    }
    return intersections;
  }

  /**
   * Inverts a list of filter ranges. It is a assumed that the ranges in the list do not overlap.
   * 
   * @param <T> the class of the filter range
   * @param ranges the ranges to invert
   * @return a list of ranges that represents the inverse of the provided ranges
   */
  public static <T extends Comparable<T>> List<FilterRange<T>> invertRanges(
      final List<FilterRange<T>> ranges) {
    Collections.sort(ranges);
    if (ranges.size() == 0) {
      return Lists.newArrayList(FilterRange.include());
    }
    final List<FilterRange<T>> newRanges = Lists.newArrayList();
    T start = null;
    boolean startInclusive = true;
    boolean exact = true;
    for (int i = 0; i < ranges.size(); i++) {
      final FilterRange<T> nextRange = ranges.get(i);
      if ((start != null) || (nextRange.getStart() != null)) {
        newRanges.add(
            FilterRange.of(
                start,
                nextRange.getStart(),
                startInclusive,
                !nextRange.startInclusive,
                exact && nextRange.exact));
      }
      start = nextRange.getEnd();
      startInclusive = !nextRange.endInclusive;
      exact = nextRange.exact;
    }
    if (start != null) {
      newRanges.add(FilterRange.of(start, null, startInclusive, true, exact));
    }
    return newRanges;
  }

}
