/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import org.locationtech.geowave.core.store.query.filter.expression.FilterRange;

/**
 * Overrides much of the logic for filter ranges to prevent constraints with different casing
 * parameters from being merged together.
 */
public class TextFilterRange extends FilterRange<String> {

  private final boolean caseSensitive;
  private final boolean reversed;

  public TextFilterRange(
      final String start,
      final String end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact,
      final boolean caseSensitive,
      final boolean reversed) {
    super(start, end, startInclusive, endInclusive, exact);
    this.caseSensitive = caseSensitive;
    this.reversed = reversed;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public boolean isReversed() {
    return reversed;
  }

  @Override
  protected boolean isAfter(final FilterRange<String> other, final boolean startPoint) {
    final TextFilterRange textRange = (TextFilterRange) other;
    if ((caseSensitive == textRange.caseSensitive) && (reversed == textRange.reversed)) {
      return super.isAfter(other, startPoint);
    }
    final int caseCompare = Boolean.compare(caseSensitive, textRange.caseSensitive);
    if (caseCompare < 0) {
      return false;
    }
    if (caseCompare > 0) {
      return true;
    }
    final int reverseCompare = Boolean.compare(reversed, textRange.reversed);
    if (reverseCompare < 0) {
      return false;
    }
    return true;
  }

  @Override
  protected boolean isBefore(final FilterRange<String> other, final boolean startPoint) {
    final TextFilterRange textRange = (TextFilterRange) other;
    if ((caseSensitive == textRange.caseSensitive) && (reversed == textRange.reversed)) {
      return super.isAfter(other, startPoint);
    }
    final int caseCompare = Boolean.compare(caseSensitive, textRange.caseSensitive);
    if (caseCompare < 0) {
      return true;
    }
    if (caseCompare > 0) {
      return false;
    }
    final int reverseCompare = Boolean.compare(reversed, textRange.reversed);
    if (reverseCompare < 0) {
      return true;
    }
    return false;
  }

  @Override
  protected boolean overlaps(final FilterRange<String> other) {
    if ((caseSensitive == ((TextFilterRange) other).caseSensitive)
        && (reversed == ((TextFilterRange) other).reversed)) {
      return super.overlaps(other);
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result) + (caseSensitive ? 1 : 0);
    result = (prime * result) + (reversed ? 1 : 0);
    return result;
  }

  @Override
  public boolean equals(final Object other) {
    if (super.equals(other) && (other instanceof TextFilterRange)) {
      final TextFilterRange otherRange = (TextFilterRange) other;
      return (caseSensitive == otherRange.caseSensitive) && (reversed == otherRange.reversed);
    }
    return false;
  }

  @Override
  public int compareTo(final FilterRange<String> o) {
    if (!(o instanceof TextFilterRange)) {
      return -1;
    }
    final TextFilterRange other = (TextFilterRange) o;
    int compare = Boolean.compare(caseSensitive, other.caseSensitive);
    if (compare == 0) {
      compare = Boolean.compare(reversed, other.reversed);
    }
    if (compare == 0) {
      return super.compareTo(other);
    }
    return compare;
  }

  public static TextFilterRange of(
      final String start,
      final String end,
      final boolean startInclusive,
      final boolean endInclusive,
      final boolean exact,
      final boolean caseSensitive,
      final boolean reversed) {
    return new TextFilterRange(
        start,
        end,
        startInclusive,
        endInclusive,
        exact,
        caseSensitive,
        reversed);
  }

}
