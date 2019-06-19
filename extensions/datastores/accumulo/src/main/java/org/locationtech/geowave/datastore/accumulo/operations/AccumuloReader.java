/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.ParallelDecoder;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.SimpleParallelDecoder;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.accumulo.AccumuloRow;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class AccumuloReader<T> implements RowReader<T> {
  private static final Logger LOGGER = Logger.getLogger(AccumuloReader.class);
  protected final ScannerBase scanner;
  private final Iterator<Entry<Key, Value>> baseIter;
  private ParallelDecoder<T> parallelDecoder = null;
  private final Iterator<T> iterator;

  private final boolean wholeRowEncoding;
  private final int partitionKeyLength;

  private Entry<Key, Value> peekedEntry = null;

  public AccumuloReader(
      final ScannerBase scanner,
      final GeoWaveRowIteratorTransformer<T> transformer,
      final int partitionKeyLength,
      final boolean wholeRowEncoding,
      final boolean clientSideRowMerging,
      final boolean parallel) {
    this.scanner = scanner;
    this.partitionKeyLength = partitionKeyLength;
    this.wholeRowEncoding = wholeRowEncoding;
    this.baseIter = scanner.iterator();

    if (parallel) {
      this.parallelDecoder =
          new SimpleParallelDecoder<>(transformer, getIterator(clientSideRowMerging));
      try {
        this.parallelDecoder.startDecode();
      } catch (final Exception e) {
        Throwables.propagate(e);
      }

      this.iterator = parallelDecoder;
    } else {
      this.iterator = transformer.apply(getIterator(clientSideRowMerging));
    }
  }

  private Iterator<GeoWaveRow> getIterator(final boolean clientSideRowMerging) {
    if (clientSideRowMerging) {
      return new MergingIterator<>(this.baseIter, this);
    } else {
      return new NonMergingIterator<>(this.baseIter, this);
    }
  }

  @Override
  public void close() {
    scanner.close();
    if (parallelDecoder != null) {
      parallelDecoder.close();
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }

  private static class MergingIterator<T> implements Iterator<GeoWaveRow> {
    private final AccumuloReader<T> parent;
    private final Iterator<Entry<Key, Value>> baseIter;

    public MergingIterator(
        final Iterator<Entry<Key, Value>> baseIter,
        final AccumuloReader<T> parent) {
      this.parent = parent;
      this.baseIter = baseIter;
    }

    @Override
    public boolean hasNext() {
      return (parent.peekedEntry != null) || baseIter.hasNext();
    }

    @Override
    public GeoWaveRow next() {
      if ((parent.peekedEntry == null) && !baseIter.hasNext()) {
        throw new NoSuchElementException();
      }
      return parent.mergingNext();
    }
  }

  private static class NonMergingIterator<T> implements Iterator<GeoWaveRow> {
    private final AccumuloReader<T> parent;
    private final Iterator<Entry<Key, Value>> baseIter;

    public NonMergingIterator(
        final Iterator<Entry<Key, Value>> baseIter,
        final AccumuloReader<T> parent) {
      this.parent = parent;
      this.baseIter = baseIter;
    }

    @Override
    public boolean hasNext() {
      return baseIter.hasNext();
    }

    @Override
    public GeoWaveRow next() {
      if (!baseIter.hasNext()) {
        throw new NoSuchElementException();
      }
      return parent.internalNext();
    }
  }

  /**
   * When row merging (client-side only), the merging iterator expects a single row w/ multiple
   * field value maps. Since Accumulo returns multiple rows w/ the same row ID, we need to combine
   * the field value maps from these separate rows into one result.
   */
  private GeoWaveRow mergingNext() {
    // Get next result from scanner
    // We may have already peeked at it
    Entry<Key, Value> nextEntry = null;
    if (peekedEntry != null) {
      nextEntry = peekedEntry;
    } else {
      nextEntry = baseIter.next();
    }
    peekedEntry = null;

    final List<Map<Key, Value>> fieldValueMapList = Lists.newLinkedList();
    fieldValueMapList.add(entryToRowMapping(nextEntry));

    // (for client-side merge only) Peek ahead to see if it needs to be
    // combined with the next result
    while (baseIter.hasNext()) {
      peekedEntry = baseIter.next();

      if (entryRowIdsMatch(nextEntry, peekedEntry)) {
        fieldValueMapList.add(entryToRowMapping(peekedEntry));
        peekedEntry = null;
      } else {
        // If we got here, we peeked at a non-matching row
        // Hold on to that in peekedEntry, and exit
        break;
      }
    }

    return new AccumuloRow(
        nextEntry.getKey().getRow().copyBytes(),
        partitionKeyLength,
        fieldValueMapList,
        wholeRowEncoding);
  }

  private GeoWaveRow internalNext() {
    final Entry<Key, Value> nextEntry = baseIter.next();

    final List<Map<Key, Value>> fieldValueMapList = Lists.newLinkedList();
    fieldValueMapList.add(entryToRowMapping(nextEntry));

    return new AccumuloRow(
        nextEntry.getKey().getRow().copyBytes(),
        partitionKeyLength,
        fieldValueMapList,
        false);
  }

  private boolean entryRowIdsMatch(
      final Entry<Key, Value> nextEntry,
      final Entry<Key, Value> peekedEntry) {
    final GeoWaveKey nextKey =
        new GeoWaveKeyImpl(nextEntry.getKey().getRow().copyBytes(), partitionKeyLength);

    final GeoWaveKey peekedKey =
        new GeoWaveKeyImpl(peekedEntry.getKey().getRow().copyBytes(), partitionKeyLength);

    return DataStoreUtils.rowIdsMatch(nextKey, peekedKey);
  }

  private Map<Key, Value> entryToRowMapping(final Entry<Key, Value> entry) {
    Map<Key, Value> rowMapping;

    if (wholeRowEncoding) {
      try {
        rowMapping = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
      } catch (final IOException e) {
        LOGGER.error(
            "Could not decode row from iterator. Ensure whole row iterators are being used.",
            e);
        return null;
      }
    } else {
      rowMapping = new HashMap<>();
      rowMapping.put(entry.getKey(), entry.getValue());
    }

    return rowMapping;
  }
}
