/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.DeferredReadCommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.UnreadFieldDataList;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;

public class MemoryDataStoreOperations implements DataStoreOperations {
  private static final Logger LOGGER = Logger.getLogger(MemoryDataStoreOperations.class);
  private final Map<String, SortedSet<MemoryStoreEntry>> storeData =
      Collections.synchronizedMap(new HashMap<String, SortedSet<MemoryStoreEntry>>());
  private final Map<MetadataType, SortedSet<MemoryMetadataEntry>> metadataStore =
      Collections.synchronizedMap(new HashMap<MetadataType, SortedSet<MemoryMetadataEntry>>());
  private final DataStoreOptions options;

  public MemoryDataStoreOperations() {
    this(new BaseDataStoreOptions() {
      @Override
      public boolean isServerSideLibraryEnabled() {
        // memory datastore doesn't have a serverside option
        return false;
      }
    });
  }

  public MemoryDataStoreOperations(final DataStoreOptions options) {
    this.options = options;
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    if (AbstractGeoWavePersistence.METADATA_TABLE.equals(indexName)) {
      return !metadataStore.isEmpty();
    }
    return storeData.containsKey(indexName);
  }

  @Override
  public void deleteAll() throws Exception {
    storeData.clear();
  }

  @Override
  public boolean deleteAll(
      final String tableName,
      final String typeName,
      final Short internalAdapterId,
      final String... additionalAuthorizations) {
    return false;
  }

  @Override
  public boolean ensureAuthorizations(final String clientUser, final String... authorizations) {
    return true;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    return new MyIndexWriter<>(index.getName());
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    return new MyIndexDeleter(indexName, authorizations);
  }

  protected SortedSet<MemoryStoreEntry> getRowsForIndex(final String id) {
    SortedSet<MemoryStoreEntry> set = storeData.get(id);
    if (set == null) {
      set = Collections.synchronizedSortedSet(new TreeSet<MemoryStoreEntry>());
      storeData.put(id, set);
    }
    return set;
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    final SortedSet<MemoryStoreEntry> internalData =
        storeData.get(readerParams.getIndex().getName());
    int counter = 0;
    List<MemoryStoreEntry> retVal = new ArrayList<>();
    final Collection<SinglePartitionQueryRanges> partitionRanges =
        readerParams.getQueryRanges().getPartitionQueryRanges();
    if ((partitionRanges == null) || partitionRanges.isEmpty()) {
      retVal.addAll(internalData);
      // remove unauthorized
      final Iterator<MemoryStoreEntry> it = retVal.iterator();
      while (it.hasNext()) {
        if (!isAuthorized(it.next(), readerParams.getAdditionalAuthorizations())) {
          it.remove();
        }
      }
      if ((readerParams.getLimit() != null)
          && (readerParams.getLimit() > 0)
          && (retVal.size() > readerParams.getLimit())) {
        retVal = retVal.subList(0, readerParams.getLimit());
      }
    } else {
      for (final SinglePartitionQueryRanges p : partitionRanges) {
        for (final ByteArrayRange r : p.getSortKeyRanges()) {
          final SortedSet<MemoryStoreEntry> set;
          if (r.isSingleValue()) {
            set =
                internalData.subSet(
                    new MemoryStoreEntry(p.getPartitionKey(), r.getStart()),
                    new MemoryStoreEntry(
                        p.getPartitionKey(),
                        ByteArrayUtils.getNextPrefix(r.getStart())));
          } else {
            set =
                internalData.tailSet(
                    new MemoryStoreEntry(p.getPartitionKey(), r.getStart())).headSet(
                        new MemoryStoreEntry(p.getPartitionKey(), r.getEndAsNextPrefix()));
          }
          // remove unauthorized
          final Iterator<MemoryStoreEntry> it = set.iterator();
          while (it.hasNext()) {
            if (!isAuthorized(it.next(), readerParams.getAdditionalAuthorizations())) {
              it.remove();
            }
          }
          if ((readerParams.getLimit() != null)
              && (readerParams.getLimit() > 0)
              && ((counter + set.size()) > readerParams.getLimit())) {
            final List<MemoryStoreEntry> subset = new ArrayList<>(set);
            retVal.addAll(subset.subList(0, readerParams.getLimit() - counter));
            break;
          } else {
            retVal.addAll(set);
            counter += set.size();
            if ((readerParams.getLimit() != null)
                && (readerParams.getLimit() > 0)
                && (counter >= readerParams.getLimit())) {
              break;
            }
          }
        }
      }
    }
    return new MyIndexReader(Iterators.filter(retVal.iterator(), new Predicate<MemoryStoreEntry>() {
      @Override
      public boolean apply(final MemoryStoreEntry input) {
        if ((readerParams.getFilter() != null) && options.isServerSideLibraryEnabled()) {
          final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<>();
          final List<FlattenedUnreadData> unreadData = new ArrayList<>();
          final List<String> commonIndexFieldNames =
              DataStoreUtils.getUniqueDimensionFields(readerParams.getIndex().getIndexModel());
          for (final GeoWaveValue v : input.getRow().getFieldValues()) {
            unreadData.add(
                DataStoreUtils.aggregateFieldData(
                    input.getRow(),
                    v,
                    commonData,
                    readerParams.getIndex().getIndexModel(),
                    commonIndexFieldNames));
          }
          return readerParams.getFilter().accept(
              readerParams.getIndex().getIndexModel(),
              new DeferredReadCommonIndexedPersistenceEncoding(
                  input.getRow().getAdapterId(),
                  input.getRow().getDataId(),
                  input.getRow().getPartitionKey(),
                  input.getRow().getSortKey(),
                  input.getRow().getNumberOfDuplicates(),
                  commonData,
                  unreadData.isEmpty() ? null : new UnreadFieldDataList(unreadData)));
        }
        return true;
      }
    }), readerParams.getRowTransformer());
  }

  private boolean isAuthorized(final MemoryStoreEntry row, final String... authorizations) {
    for (final GeoWaveValue value : row.getRow().getFieldValues()) {
      if (!MemoryStoreUtils.isAuthorized(value.getVisibility(), authorizations)) {
        return false;
      }
    }
    return true;
  }

  private static class MyIndexReader<T> implements RowReader<T> {
    private final Iterator<T> it;

    public MyIndexReader(
        final Iterator<MemoryStoreEntry> it,
        final GeoWaveRowIteratorTransformer<T> rowTransformer) {
      super();
      this.it = rowTransformer.apply(Iterators.transform(it, e -> e.row));
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public T next() {
      return it.next();
    }
  }

  private class MyIndexWriter<T> implements RowWriter {
    final String indexName;

    public MyIndexWriter(final String indexName) {
      super();
      this.indexName = indexName;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void flush() {
      try {
        close();
      } catch (final IOException e) {
        LOGGER.error("Error closing index writer", e);
      }
    }

    @Override
    public void write(final GeoWaveRow[] rows) {
      for (final GeoWaveRow r : rows) {
        write(r);
      }
    }

    @Override
    public void write(final GeoWaveRow row) {
      SortedSet<MemoryStoreEntry> rowTreeSet = storeData.get(indexName);
      if (rowTreeSet == null) {
        rowTreeSet = new TreeSet<>();
        storeData.put(indexName, rowTreeSet);
      }
      if (rowTreeSet.contains(new MemoryStoreEntry(row))) {
        rowTreeSet.remove(new MemoryStoreEntry(row));
      }
      if (!rowTreeSet.add(new MemoryStoreEntry(row))) {
        LOGGER.warn("Unable to add new entry");
      }
    }
  }

  private class MyIndexDeleter implements RowDeleter {
    private final String indexName;
    private final String[] authorizations;

    public MyIndexDeleter(final String indexName, final String... authorizations) {
      this.indexName = indexName;
      this.authorizations = authorizations;
    }

    @Override
    public void close() {}

    @Override
    public void delete(final GeoWaveRow row) {
      final MemoryStoreEntry entry = new MemoryStoreEntry(row);
      if (isAuthorized(entry, authorizations)) {
        final SortedSet<MemoryStoreEntry> rowTreeSet = storeData.get(indexName);
        if (rowTreeSet != null) {
          if (!rowTreeSet.remove(entry)) {
            LOGGER.warn("Unable to remove entry");
          }
        }
      }
    }

    @Override
    public void flush() {
      // Do nothing, delete is done immediately.
    }
  }

  public static class MemoryStoreEntry implements Comparable<MemoryStoreEntry> {
    private final GeoWaveRow row;

    public MemoryStoreEntry(final byte[] comparisonPartitionKey, final byte[] comparisonSortKey) {
      row =
          new GeoWaveRowImpl(
              new GeoWaveKeyImpl(
                  new byte[] {0},
                  (short) 0, // new byte[] {},
                  comparisonPartitionKey,
                  comparisonSortKey,
                  0),
              null);
    }

    public MemoryStoreEntry(final GeoWaveRow row) {
      this.row = row;
    }

    public GeoWaveRow getRow() {
      return row;
    }

    public byte[] getCompositeInsertionId() {
      return ((GeoWaveKeyImpl) ((GeoWaveRowImpl) row).getKey()).getCompositeInsertionId();
    }

    @Override
    public int compareTo(final MemoryStoreEntry other) {
      final int indexIdCompare =
          UnsignedBytes.lexicographicalComparator().compare(
              getCompositeInsertionId(),
              other.getCompositeInsertionId());
      if (indexIdCompare != 0) {
        return indexIdCompare;
      }
      final int dataIdCompare =
          UnsignedBytes.lexicographicalComparator().compare(
              row.getDataId(),
              other.getRow().getDataId());
      if (dataIdCompare != 0) {
        return dataIdCompare;
      }
      final int adapterIdCompare =
          UnsignedBytes.lexicographicalComparator().compare(
              ByteArrayUtils.shortToByteArray(row.getAdapterId()),
              ByteArrayUtils.shortToByteArray(other.getRow().getAdapterId()));
      if (adapterIdCompare != 0) {
        return adapterIdCompare;
      }
      return 0;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((row == null) ? 0 : row.hashCode());
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
      final MemoryStoreEntry other = (MemoryStoreEntry) obj;
      if (row == null) {
        if (other.row != null) {
          return false;
        }
      }
      return compareTo(other) == 0;
    }
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    return new MyMetadataWriter<>(metadataType);
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return new MyMetadataReader(metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    return new MyMetadataDeleter(metadataType);
  }

  private class MyMetadataReader implements MetadataReader {
    protected final MetadataType type;

    public MyMetadataReader(final MetadataType type) {
      super();
      this.type = type;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
      final SortedSet<MemoryMetadataEntry> typeStore = metadataStore.get(type);
      if (typeStore == null) {
        return new CloseableIterator.Empty<>();
      }
      final SortedSet<MemoryMetadataEntry> set =
          typeStore.subSet(
              new MemoryMetadataEntry(
                  new GeoWaveMetadata(query.getPrimaryId(), query.getSecondaryId(), null, null),
                  null),
              new MemoryMetadataEntry(
                  new GeoWaveMetadata(
                      getNextPrefix(query.getPrimaryId()),
                      getNextPrefix(query.getSecondaryId()),
                      // this should be sufficient
                      new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
                      // this should be sufficient
                      new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
                  new byte[] {
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF}));
      Iterator<MemoryMetadataEntry> it = set.iterator();
      if ((query.getAuthorizations() != null) && (query.getAuthorizations().length > 0)) {
        it =
            Iterators.filter(
                it,
                input -> MemoryStoreUtils.isAuthorized(
                    input.getMetadata().getVisibility(),
                    query.getAuthorizations()));
      }
      final Iterator<GeoWaveMetadata> itTransformed =
          Iterators.transform(it, input -> input.metadata);
      if (MetadataType.STATS.equals(type)) {
        return new CloseableIterator.Wrapper(new Iterator<GeoWaveMetadata>() {
          final PeekingIterator<GeoWaveMetadata> peekingIt =
              Iterators.peekingIterator(itTransformed);

          @Override
          public boolean hasNext() {
            return peekingIt.hasNext();
          }

          @Override
          public GeoWaveMetadata next() {
            InternalDataStatistics currentStat = null;
            GeoWaveMetadata currentMetadata = null;
            byte[] vis = null;
            while (peekingIt.hasNext()) {
              currentMetadata = peekingIt.next();
              vis = currentMetadata.getVisibility();
              if (!peekingIt.hasNext()) {
                break;
              }
              final GeoWaveMetadata next = peekingIt.peek();
              if (Objects.deepEquals(currentMetadata.getPrimaryId(), next.getPrimaryId())
                  && Objects.deepEquals(currentMetadata.getSecondaryId(), next.getSecondaryId())) {
                if (currentStat == null) {
                  currentStat =
                      (InternalDataStatistics) PersistenceUtils.fromBinary(
                          currentMetadata.getValue());
                }
                currentStat.merge((Mergeable) PersistenceUtils.fromBinary(next.getValue()));
                vis = combineVisibilities(vis, next.getVisibility());
              } else {
                break;
              }
            }
            if (currentStat == null) {
              return currentMetadata;
            }
            return new GeoWaveMetadata(
                currentMetadata.getPrimaryId(),
                currentMetadata.getSecondaryId(),
                vis,
                PersistenceUtils.toBinary(currentStat));
          }
        });
      }
      // convert to and from array just to avoid concurrent modification
      // issues on the iterator that is linked back to the metadataStore
      // sortedSet (basically clone the iterator, so for example deletes
      // can occur while iterating through this query result)
      return new CloseableIterator.Wrapper(
          Iterators.forArray(Iterators.toArray(itTransformed, GeoWaveMetadata.class)));
    }
  }

  private static final byte[] AMPRISAND = StringUtils.stringToBinary("&");

  private static byte[] combineVisibilities(final byte[] vis1, final byte[] vis2) {
    if ((vis1 == null) || (vis1.length == 0)) {
      return vis2;
    }
    if ((vis2 == null) || (vis2.length == 0)) {
      return vis1;
    }
    return ArrayUtils.addAll(ArrayUtils.addAll(vis1, AMPRISAND), vis2);
  }

  private static byte[] getNextPrefix(final byte[] bytes) {
    if (bytes == null) {
      // this is simply for memory data store test purposes and is just an
      // attempt to go to the end of the memory datastore table
      return new byte[] {
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,
          (byte) 0xFF,};
    }
    return new ByteArray(bytes).getNextPrefix();
  }

  private class MyMetadataWriter<T> implements MetadataWriter {
    private final MetadataType type;

    public MyMetadataWriter(final MetadataType type) {
      super();
      this.type = type;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void flush() {
      try {
        close();
      } catch (final IOException e) {
        LOGGER.error("Error closing metadata writer", e);
      }
    }

    @Override
    public void write(final GeoWaveMetadata metadata) {
      SortedSet<MemoryMetadataEntry> typeStore = metadataStore.get(type);
      if (typeStore == null) {
        typeStore = new TreeSet<>();
        metadataStore.put(type, typeStore);
      }
      if (typeStore.contains(new MemoryMetadataEntry(metadata))) {
        typeStore.remove(new MemoryMetadataEntry(metadata));
      }
      if (!typeStore.add(new MemoryMetadataEntry(metadata))) {
        LOGGER.warn("Unable to add new metadata");
      }
    }
  }

  private class MyMetadataDeleter extends MyMetadataReader implements MetadataDeleter {
    public MyMetadataDeleter(final MetadataType type) {
      super(type);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean delete(final MetadataQuery query) {
      final List<GeoWaveMetadata> toRemove = new ArrayList<>();
      try (CloseableIterator<GeoWaveMetadata> it = query(query)) {
        while (it.hasNext()) {
          toRemove.add(it.next());
        }
      }
      for (final GeoWaveMetadata r : toRemove) {
        metadataStore.get(type).remove(new MemoryMetadataEntry(r));
      }
      return true;
    }

    @Override
    public void flush() {}
  }

  public static class MemoryMetadataEntry implements Comparable<MemoryMetadataEntry> {
    private final GeoWaveMetadata metadata;
    // this is just to allow storing duplicates in the treemap
    private final byte[] uuidBytes;

    public MemoryMetadataEntry(final GeoWaveMetadata metadata) {
      this(metadata, UUID.randomUUID().toString().getBytes(StringUtils.getGeoWaveCharset()));
    }

    public MemoryMetadataEntry(final GeoWaveMetadata metadata, final byte[] uuidBytes) {
      this.metadata = metadata;
      this.uuidBytes = uuidBytes;
    }

    public GeoWaveMetadata getMetadata() {
      return metadata;
    }

    @Override
    public int compareTo(final MemoryMetadataEntry other) {
      final Comparator<byte[]> lexyWithNullHandling =
          Ordering.from(UnsignedBytes.lexicographicalComparator()).nullsFirst();
      final int primaryIdCompare =
          lexyWithNullHandling.compare(metadata.getPrimaryId(), other.metadata.getPrimaryId());
      if (primaryIdCompare != 0) {
        return primaryIdCompare;
      }
      final int secondaryIdCompare =
          lexyWithNullHandling.compare(metadata.getSecondaryId(), other.metadata.getSecondaryId());
      if (secondaryIdCompare != 0) {
        return secondaryIdCompare;
      }
      final int visibilityCompare =
          lexyWithNullHandling.compare(metadata.getVisibility(), other.metadata.getVisibility());
      if (visibilityCompare != 0) {
        return visibilityCompare;
      }
      // this is just to allow storing duplicates in the treemap
      return lexyWithNullHandling.compare(uuidBytes, other.uuidBytes);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((metadata == null) ? 0 : metadata.hashCode());
      result = (prime * result) + Arrays.hashCode(uuidBytes);
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
      final MemoryMetadataEntry other = (MemoryMetadataEntry) obj;
      if (metadata == null) {
        if (other.metadata != null) {
          return false;
        }
      }
      return compareTo(other) == 0;
    }
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    return true;
  }
}
