/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SinglePartitionQueryRanges {
  private final byte[] partitionKey;

  private final Collection<ByteArrayRange> sortKeyRanges;

  public SinglePartitionQueryRanges(
      final byte[] partitionKey,
      final Collection<ByteArrayRange> sortKeyRanges) {
    this.partitionKey = partitionKey;
    this.sortKeyRanges = sortKeyRanges;
  }

  public SinglePartitionQueryRanges(final byte[] partitionKey) {
    this.partitionKey = partitionKey;
    sortKeyRanges = null;
  }

  public SinglePartitionQueryRanges(final List<ByteArrayRange> sortKeyRanges) {
    this.sortKeyRanges = sortKeyRanges;
    partitionKey = null;
  }

  public SinglePartitionQueryRanges(final ByteArrayRange singleSortKeyRange) {
    sortKeyRanges = Collections.singletonList(singleSortKeyRange);
    partitionKey = null;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public Collection<ByteArrayRange> getSortKeyRanges() {
    return sortKeyRanges;
  }

  public ByteArrayRange getSingleRange() {
    byte[] start = null;
    byte[] end = null;

    for (final ByteArrayRange range : sortKeyRanges) {
      if ((start == null) || (ByteArrayUtils.compare(range.getStart(), start) < 0)) {
        start = range.getStart();
      }
      if ((end == null) || (ByteArrayUtils.compare(range.getEnd(), end) > 0)) {
        end = range.getEnd();
      }
    }
    return new ByteArrayRange(start, end);
  }
}
