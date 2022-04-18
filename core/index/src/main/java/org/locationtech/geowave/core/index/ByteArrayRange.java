/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** * Defines a unit interval on a number line */
public class ByteArrayRange implements Comparable<ByteArrayRange> {
  protected byte[] start;
  protected byte[] end;
  protected boolean singleValue;

  /**
   * *
   *
   * @param start start of unit interval
   * @param end end of unit interval
   */
  public ByteArrayRange(final byte[] start, final byte[] end) {
    this(start, end, false);
  }

  /**
   * *
   *
   * @param start start of unit interval
   * @param end end of unit interval
   */
  public ByteArrayRange(final byte[] start, final byte[] end, final boolean singleValue) {
    this.start = start;
    this.end = end;
    this.singleValue = singleValue;
  }

  public byte[] getStart() {
    return start;
  }

  public byte[] getEnd() {
    return end;
  }

  public byte[] getStartAsPreviousPrefix() {
    if (start == null) {
      return null;
    }
    return ByteArrayUtils.getPreviousPrefix(start);
  }

  public byte[] getEndAsNextPrefix() {
    if (end == null) {
      return null;
    }
    return ByteArrayUtils.getNextPrefix(end);
  }

  public boolean isSingleValue() {
    return singleValue;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((end == null) ? 0 : Arrays.hashCode(end));
    result = (prime * result) + (singleValue ? 1231 : 1237);
    result = (prime * result) + ((start == null) ? 0 : Arrays.hashCode(start));
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ByteArrayRange other = (ByteArrayRange) obj;
    if (end == null) {
      if (other.end != null) {
        return false;
      }
    } else if (!Arrays.equals(end, other.end)) {
      return false;
    }
    if (singleValue != other.singleValue) {
      return false;
    }
    if (start == null) {
      if (other.start != null) {
        return false;
      }
    } else if (!Arrays.equals(start, other.start)) {
      return false;
    }
    return true;
  }

  public boolean intersects(final ByteArrayRange other) {
    if (isSingleValue()) {
      if (other.isSingleValue()) {
        return Arrays.equals(getStart(), other.getStart());
      }
      return false;
    }
    return ((ByteArrayUtils.compare(getStart(), other.getEndAsNextPrefix()) < 0)
        && (ByteArrayUtils.compare(getEndAsNextPrefix(), other.getStart()) > 0));
  }

  public ByteArrayRange intersection(final ByteArrayRange other) {
    return new ByteArrayRange(
        ByteArrayUtils.compare(start, other.start) <= 0 ? other.start : start,
        ByteArrayUtils.compare(getEndAsNextPrefix(), other.getEndAsNextPrefix()) >= 0 ? other.end
            : end);
  }

  public ByteArrayRange union(final ByteArrayRange other) {
    return new ByteArrayRange(
        ByteArrayUtils.compare(start, other.start) <= 0 ? start : other.start,
        ByteArrayUtils.compare(getEndAsNextPrefix(), other.getEndAsNextPrefix()) >= 0 ? end
            : other.end);
  }

  @Override
  public int compareTo(final ByteArrayRange other) {
    final int diff = ByteArrayUtils.compare(getStart(), other.getStart());
    return diff != 0 ? diff
        : ByteArrayUtils.compare(getEndAsNextPrefix(), other.getEndAsNextPrefix());
  }

  public static enum MergeOperation {
    UNION, INTERSECTION
  }

  public static final Collection<ByteArrayRange> mergeIntersections(
      final Collection<ByteArrayRange> ranges,
      final MergeOperation op) {
    final List<ByteArrayRange> rangeList = new ArrayList<>(ranges);
    // sort order so the first range can consume following ranges
    Collections.<ByteArrayRange>sort(rangeList);
    final List<ByteArrayRange> result = new ArrayList<>();
    for (int i = 0; i < rangeList.size();) {
      ByteArrayRange r1 = rangeList.get(i);
      int j = i + 1;
      for (; j < rangeList.size(); j++) {
        final ByteArrayRange r2 = rangeList.get(j);
        if (r1.intersects(r2)) {
          if (op.equals(MergeOperation.UNION)) {
            r1 = r1.union(r2);
          } else {
            r1 = r1.intersection(r2);
          }
        } else {
          break;
        }
      }
      i = j;
      result.add(r1);
    }
    return result;
  }
}
