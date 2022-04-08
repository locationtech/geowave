/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.bigtable.operations;

import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.datastore.hbase.operations.HBaseOperations;
import org.locationtech.geowave.datastore.hbase.operations.HBaseReader;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import com.google.common.collect.Iterators;

public class BigtableReader<T> extends HBaseReader<T> {

  public BigtableReader(ReaderParams<T> readerParams, HBaseOperations operations) {
    super(readerParams, operations);
  }

  public BigtableReader(
      final RecordReaderParams recordReaderParams,
      final HBaseOperations operations) {
    super(recordReaderParams, operations);
  }

  @Override
  protected Iterator<T> getScanIterator(final Iterator<Result> iterable) {
    if (readerParams.getQueryRanges() != null) {
      final List<ByteArrayRange> queryRanges =
          readerParams.getQueryRanges().getCompositeQueryRanges();
      if (queryRanges != null && queryRanges.size() > 1) {
        // If we're scanning multiple ranges, add a client-side byte array range filter to prevent
        // extra rows from being returned
        return super.getScanIterator(Iterators.filter(iterable, result -> {
          return ByteArrayUtils.matchesPrefixRanges(result.getRow(), queryRanges);
        }));
      }
    }
    return super.getScanIterator(iterable);
  }

}
