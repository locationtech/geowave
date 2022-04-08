/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.cassandra.CassandraRow;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

public class CassandraReader<T> implements RowReader<T> {
  private final ReaderParams<T> readerParams;
  private final RecordReaderParams recordReaderParams;
  private final CassandraOperations operations;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;

  private CloseableIterator<T> iterator;
  private final boolean visibilityEnabled;

  public CassandraReader(
      final ReaderParams<T> readerParams,
      final CassandraOperations operations,
      final boolean visibilityEnabled) {
    this.readerParams = readerParams;
    recordReaderParams = null;
    this.operations = operations;
    this.rowTransformer = readerParams.getRowTransformer();
    this.visibilityEnabled = visibilityEnabled;

    initScanner();
  }

  public CassandraReader(
      final RecordReaderParams recordReaderParams,
      final CassandraOperations operations,
      final boolean visibilityEnabled) {
    readerParams = null;
    this.recordReaderParams = recordReaderParams;
    this.operations = operations;
    this.visibilityEnabled = visibilityEnabled;
    this.rowTransformer =
        (GeoWaveRowIteratorTransformer<T>) GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER;

    initRecordScanner();
  }

  @SuppressWarnings("unchecked")
  private CloseableIterator<T> wrapResults(
      final CloseableIterator<CassandraRow> results,
      final RangeReaderParams<T> readerParams) {

    final Set<String> authorizations = Sets.newHashSet(readerParams.getAdditionalAuthorizations());
    final Iterator<GeoWaveRow> iterator =
        (Iterator) Streams.stream(results).filter(
            new ClientVisibilityFilter(authorizations)).iterator();
    return new CloseableIteratorWrapper<>(
        results,
        rowTransformer.apply(
            DataStoreUtils.isMergingIteratorRequired(readerParams, visibilityEnabled)
                ? new GeoWaveRowMergingIterator(iterator)
                : iterator));
  }

  protected void initScanner() {
    final Collection<SinglePartitionQueryRanges> ranges =
        readerParams.getQueryRanges().getPartitionQueryRanges();
    if ((ranges != null) && !ranges.isEmpty()) {
      iterator =
          operations.getBatchedRangeRead(
              readerParams.getIndex().getName(),
              readerParams.getAdapterIds(),
              ranges,
              DataStoreUtils.isMergingIteratorRequired(readerParams, visibilityEnabled),
              rowTransformer,
              new ClientVisibilityFilter(
                  Sets.newHashSet(readerParams.getAdditionalAuthorizations()))).results();
    } else {
      // TODO figure out the query select by adapter IDs here
      final Select select = operations.getSelect(readerParams.getIndex().getName());
      CloseableIterator<CassandraRow> results = operations.executeQuery(select.build());
      if ((readerParams.getAdapterIds() != null) && (readerParams.getAdapterIds().length > 0)) {
        // TODO because we aren't filtering server-side by adapter ID,
        // we will need to filter here on the client
        results =
            new CloseableIteratorWrapper<>(
                results,
                Iterators.filter(
                    results,
                    input -> ArrayUtils.contains(
                        readerParams.getAdapterIds(),
                        input.getAdapterId())));
      }
      iterator = wrapResults(results, readerParams);
    }
  }

  protected void initRecordScanner() {
    final short[] adapterIds =
        recordReaderParams.getAdapterIds() != null ? recordReaderParams.getAdapterIds()
            : new short[0];

    final GeoWaveRowRange range = recordReaderParams.getRowRange();
    final byte[] startKey = range.isInfiniteStartSortKey() ? null : range.getStartSortKey();
    final byte[] stopKey = range.isInfiniteStopSortKey() ? null : range.getEndSortKey();
    final SinglePartitionQueryRanges partitionRange =
        new SinglePartitionQueryRanges(
            range.getPartitionKey(),
            Collections.singleton(new ByteArrayRange(startKey, stopKey)));
    final Set<String> authorizations =
        Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations());
    iterator =
        operations.getBatchedRangeRead(
            recordReaderParams.getIndex().getName(),
            adapterIds,
            Collections.singleton(partitionRange),
            DataStoreUtils.isMergingIteratorRequired(recordReaderParams, visibilityEnabled),
            rowTransformer,
            new ClientVisibilityFilter(authorizations)).results();
  }

  @Override
  public void close() {
    iterator.close();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }
}
