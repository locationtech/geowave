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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArrayRange.MergeOperation;

public class QueryRanges {

  private final Collection<SinglePartitionQueryRanges> partitionRanges;
  private List<ByteArrayRange> compositeQueryRanges;

  public QueryRanges() {
    // this implies an infinite range
    partitionRanges = null;
  }

  public QueryRanges(final byte[][] partitionKeys, final QueryRanges queryRanges) {
    if ((queryRanges == null)
        || (queryRanges.partitionRanges == null)
        || queryRanges.partitionRanges.isEmpty()) {
      partitionRanges = fromPartitionKeys(partitionKeys);
    } else if ((partitionKeys == null) || (partitionKeys.length == 0)) {
      partitionRanges = queryRanges.partitionRanges;
    } else {
      partitionRanges = new ArrayList<>(partitionKeys.length * queryRanges.partitionRanges.size());
      for (final byte[] partitionKey : partitionKeys) {
        for (final SinglePartitionQueryRanges sortKeyRange : queryRanges.partitionRanges) {
          byte[] newPartitionKey;
          if (partitionKey == null) {
            newPartitionKey = sortKeyRange.getPartitionKey();
          } else if (sortKeyRange.getPartitionKey() == null) {
            newPartitionKey = partitionKey;
          } else {
            newPartitionKey =
                ByteArrayUtils.combineArrays(partitionKey, sortKeyRange.getPartitionKey());
          }
          partitionRanges.add(
              new SinglePartitionQueryRanges(newPartitionKey, sortKeyRange.getSortKeyRanges()));
        }
      }
    }
  }

  public QueryRanges(final List<QueryRanges> queryRangesList) {
    // group by partition
    final Map<ByteArray, Collection<ByteArrayRange>> sortRangesPerPartition = new HashMap<>();
    for (final QueryRanges qr : queryRangesList) {
      for (final SinglePartitionQueryRanges r : qr.getPartitionQueryRanges()) {
        final Collection<ByteArrayRange> ranges =
            sortRangesPerPartition.get(new ByteArray(r.getPartitionKey()));
        if (ranges == null) {
          sortRangesPerPartition.put(
              new ByteArray(r.getPartitionKey()),
              new ArrayList<>(r.getSortKeyRanges()));
        } else {
          ranges.addAll(r.getSortKeyRanges());
        }
      }
    }
    partitionRanges = new ArrayList<>(sortRangesPerPartition.size());
    for (final Entry<ByteArray, Collection<ByteArrayRange>> e : sortRangesPerPartition.entrySet()) {
      Collection<ByteArrayRange> mergedRanges;
      if (e.getValue() != null) {
        mergedRanges = ByteArrayRange.mergeIntersections(e.getValue(), MergeOperation.UNION);
      } else {
        mergedRanges = null;
      }
      partitionRanges.add(new SinglePartitionQueryRanges(e.getKey().getBytes(), mergedRanges));
    }
  }

  public QueryRanges(final Collection<SinglePartitionQueryRanges> partitionRanges) {
    this.partitionRanges = partitionRanges;
  }

  public QueryRanges(final ByteArrayRange singleSortKeyRange) {
    partitionRanges = Collections.singletonList(new SinglePartitionQueryRanges(singleSortKeyRange));
  }

  public QueryRanges(final byte[][] partitionKeys) {
    partitionRanges = fromPartitionKeys(partitionKeys);
  }

  public boolean isEmpty() {
    return partitionRanges == null || partitionRanges.size() == 0;
  }

  private static Collection<SinglePartitionQueryRanges> fromPartitionKeys(
      final byte[][] partitionKeys) {
    if (partitionKeys == null) {
      return null;
    }
    return Arrays.stream(partitionKeys).map(input -> new SinglePartitionQueryRanges(input)).collect(
        Collectors.toList());
  }

  public Collection<SinglePartitionQueryRanges> getPartitionQueryRanges() {
    return partitionRanges;
  }

  public List<ByteArrayRange> getCompositeQueryRanges() {
    if (partitionRanges == null) {
      return null;
    }
    if (compositeQueryRanges != null) {
      return compositeQueryRanges;
    }
    if (partitionRanges.isEmpty()) {
      compositeQueryRanges = new ArrayList<>();
      return compositeQueryRanges;
    }
    final List<ByteArrayRange> internalQueryRanges = new ArrayList<>();
    for (final SinglePartitionQueryRanges partition : partitionRanges) {
      if ((partition.getSortKeyRanges() == null) || partition.getSortKeyRanges().isEmpty()) {
        internalQueryRanges.add(
            new ByteArrayRange(partition.getPartitionKey(), partition.getPartitionKey()));
      } else if (partition.getPartitionKey() == null) {
        internalQueryRanges.addAll(partition.getSortKeyRanges());
      } else {
        for (final ByteArrayRange sortKeyRange : partition.getSortKeyRanges()) {
          internalQueryRanges.add(
              new ByteArrayRange(
                  ByteArrayUtils.combineArrays(
                      partition.getPartitionKey(),
                      sortKeyRange.getStart()),
                  ByteArrayUtils.combineArrays(partition.getPartitionKey(), sortKeyRange.getEnd()),
                  sortKeyRange.singleValue));
        }
      }
    }

    compositeQueryRanges = internalQueryRanges;
    return compositeQueryRanges;
  }

  public boolean isMultiRange() {
    if (compositeQueryRanges != null) {
      return compositeQueryRanges.size() >= 2;
    }
    if (partitionRanges.isEmpty()) {
      return false;
    }
    if (partitionRanges.size() > 1) {
      return true;
    }
    final SinglePartitionQueryRanges partition = partitionRanges.iterator().next();
    if ((partition.getSortKeyRanges() != null) && (partition.getSortKeyRanges().size() <= 1)) {
      return false;
    }
    return true;
  }
}
