/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.splits;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterStoreWrapper;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.base.BaseQueryOptions;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.input.AsyncInputFormatIteratorWrapper;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.input.InputFormatIteratorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is used by the GeoWaveInputFormat to read data from a GeoWave data store.
 *
 * @param <T> The native type for the reader
 */
public class GeoWaveRecordReader<T> extends RecordReader<GeoWaveInputKey, T> {

  protected static class ProgressPerRange {
    private final float startProgress;
    private final float deltaProgress;

    public ProgressPerRange(final float startProgress, final float endProgress) {
      this.startProgress = startProgress;
      deltaProgress = endProgress - startProgress;
    }

    public float getOverallProgress(final float rangeProgress) {
      return startProgress + (rangeProgress * deltaProgress);
    }
  }

  protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveRecordReader.class);
  protected long numKeysRead;
  protected CloseableIterator<?> iterator;
  protected Map<RangeLocationPair, ProgressPerRange> progressPerRange;
  protected GeoWaveInputKey currentGeoWaveKey = null;
  protected RangeLocationPair currentGeoWaveRangeIndexPair = null;
  protected T currentValue = null;
  protected GeoWaveInputSplit split;
  protected QueryConstraints constraints;
  protected BaseQueryOptions sanitizedQueryOptions;
  protected boolean isOutputWritable;
  protected TransientAdapterStore adapterStore;
  protected InternalAdapterStore internalAdapterStore;
  protected AdapterIndexMappingStore aimStore;
  protected IndexStore indexStore;
  protected BaseDataStore dataStore;
  protected MapReduceDataStoreOperations operations;
  protected int dataIndexBatchSize;

  public GeoWaveRecordReader(
      final CommonQueryOptions commonOptions,
      final DataTypeQueryOptions<?> typeOptions,
      final IndexQueryOptions indexOptions,
      final QueryConstraints constraints,
      final boolean isOutputWritable,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore aimStore,
      final IndexStore indexStore,
      final MapReduceDataStoreOperations operations,
      final int dataIndexBatchSize) {
    this.constraints = constraints;
    // all queries will use the same instance of the dedupe filter for
    // client side filtering because the filter needs to be applied across
    // indices
    sanitizedQueryOptions =
        new BaseQueryOptions(
            commonOptions,
            typeOptions,
            indexOptions,
            new AdapterStoreWrapper(adapterStore, internalAdapterStore),
            internalAdapterStore);
    this.isOutputWritable = isOutputWritable;
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
    this.aimStore = aimStore;
    this.indexStore = indexStore;
    this.operations = operations;
    this.dataIndexBatchSize = dataIndexBatchSize;
  }

  /** Initialize a scanner over the given input split using this task attempt configuration. */
  @Override
  public void initialize(final InputSplit inSplit, final TaskAttemptContext attempt)
      throws IOException {
    split = (GeoWaveInputSplit) inSplit;

    numKeysRead = 0;

    final Set<String> indices = split.getIndexNames();
    final BigDecimal sum = BigDecimal.ZERO;

    final Map<RangeLocationPair, BigDecimal> incrementalRangeSums = new LinkedHashMap<>();
    final List<CloseableIterator<Pair<GeoWaveInputKey, T>>> allIterators = new ArrayList<>();
    final NextRangeCallback callback = new InternalCallback();
    final short[] adapters;
    // do a check for AdapterAndIndexBasedQueryConstraints in case
    // the splits provider was unable to set it
    if (constraints instanceof AdapterAndIndexBasedQueryConstraints) {
      adapters = sanitizedQueryOptions.getAdapterIds(internalAdapterStore);
    } else {
      adapters = null;
    }
    for (final String i : indices) {
      final SplitInfo splitInfo = split.getInfo(i);
      List<QueryFilter> queryFilters = null;
      if (constraints != null) {
        // if adapters isn't null that also means this constraint is
        // AdapterAndIndexBasedQueryConstraints
        if (adapters != null) {
          InternalDataAdapter<?> adapter = null;
          if (adapters.length > 1) {
            // this should be a rare situation, but just in case, loop over adapters and fill the
            // iterator of results per adapter
            for (final short adapterId : adapters) {
              final String typeName = internalAdapterStore.getTypeName(adapterId);
              if (typeName != null) {
                final DataTypeAdapter<?> baseAdapter = adapterStore.getAdapter(typeName);
                if (baseAdapter != null) {
                  adapter = baseAdapter.asInternalAdapter(adapterId);
                }
              }

              if (adapter == null) {
                LOGGER.warn("Unable to find type matching an adapter dependent query");
              }
              queryFilters =
                  ((AdapterAndIndexBasedQueryConstraints) constraints).createQueryConstraints(
                      adapter,
                      splitInfo.getIndex(),
                      aimStore.getMapping(adapterId, splitInfo.getIndex().getName())).createFilters(
                          splitInfo.getIndex());
              sanitizedQueryOptions.setAdapterId(adapterId);
              fillIterators(
                  allIterators,
                  splitInfo,
                  queryFilters,
                  sum,
                  incrementalRangeSums,
                  callback);
            }
            continue;
          }

          // in practice this is used for CQL and you can't have
          // multiple types/adapters
          if (adapters.length == 1) {
            final String typeName = internalAdapterStore.getTypeName(adapters[0]);
            if (typeName != null) {
              final DataTypeAdapter<?> baseAdapter = adapterStore.getAdapter(typeName);
              if (baseAdapter != null) {
                adapter = baseAdapter.asInternalAdapter(adapters[0]);
              }
            }
          }
          if (adapter == null) {
            LOGGER.warn("Unable to find type matching an adapter dependent query");
          }
          final QueryConstraints tempConstraints =
              ((AdapterAndIndexBasedQueryConstraints) constraints).createQueryConstraints(
                  adapter,
                  splitInfo.getIndex(),
                  adapter != null
                      ? aimStore.getMapping(adapter.getAdapterId(), splitInfo.getIndex().getName())
                      : null);
          if (tempConstraints == null) {
            LOGGER.warn(
                "Adapter and Index based constraints not satisfied for adapter '"
                    + adapter.getTypeName()
                    + "'");
            continue;
          } else {
            constraints = tempConstraints;
          }
        }

        queryFilters = constraints.createFilters(splitInfo.getIndex());
      }
      fillIterators(allIterators, splitInfo, queryFilters, sum, incrementalRangeSums, callback);
    }
    // finally we can compute percent progress
    progressPerRange = new LinkedHashMap<>();
    RangeLocationPair prevRangeIndex = null;
    float prevProgress = 0f;
    if (sum.compareTo(BigDecimal.ZERO) > 0) {
      try {
        for (final Entry<RangeLocationPair, BigDecimal> entry : incrementalRangeSums.entrySet()) {
          final BigDecimal value = entry.getValue();
          final float progress = value.divide(sum, RoundingMode.HALF_UP).floatValue();
          if (prevRangeIndex != null) {
            progressPerRange.put(prevRangeIndex, new ProgressPerRange(prevProgress, progress));
          }
          prevRangeIndex = entry.getKey();
          prevProgress = progress;
        }
        progressPerRange.put(prevRangeIndex, new ProgressPerRange(prevProgress, 1f));

      } catch (final Exception e) {
        LOGGER.warn("Unable to calculate progress", e);
      }
    }
    // concatenate iterators
    iterator = new CloseableIteratorWrapper<>(new Closeable() {
      @Override
      public void close() throws IOException {
        for (final CloseableIterator<Pair<GeoWaveInputKey, T>> reader : allIterators) {
          reader.close();
        }
      }
    }, Iterators.concat(allIterators.iterator()));


  }

  private void fillIterators(
      final List<CloseableIterator<Pair<GeoWaveInputKey, T>>> allIterators,
      final SplitInfo splitInfo,
      final List<QueryFilter> queryFilters,
      BigDecimal sum,
      final Map<RangeLocationPair, BigDecimal> incrementalRangeSums,
      final NextRangeCallback callback) {

    if (!splitInfo.getRangeLocationPairs().isEmpty()) {
      final QueryFilter[] filters =
          ((queryFilters == null) || queryFilters.isEmpty()) ? null
              : queryFilters.toArray(new QueryFilter[0]);

      final PersistentAdapterStore persistentAdapterStore =
          new AdapterStoreWrapper(adapterStore, internalAdapterStore);
      final DataIndexRetrieval dataIndexRetrieval =
          DataIndexUtils.getDataIndexRetrieval(
              operations,
              persistentAdapterStore,
              aimStore,
              internalAdapterStore,
              splitInfo.getIndex(),
              sanitizedQueryOptions.getFieldIdsAdapterPair(),
              sanitizedQueryOptions.getAggregation(),
              sanitizedQueryOptions.getAuthorizations(),
              dataIndexBatchSize);

      final List<Pair<RangeLocationPair, RowReader<GeoWaveRow>>> indexReaders =
          new ArrayList<>(splitInfo.getRangeLocationPairs().size());
      for (final RangeLocationPair r : splitInfo.getRangeLocationPairs()) {
        indexReaders.add(
            Pair.of(
                r,
                operations.createReader(
                    new RecordReaderParams(
                        splitInfo.getIndex(),
                        persistentAdapterStore,
                        aimStore,
                        internalAdapterStore,
                        sanitizedQueryOptions.getAdapterIds(internalAdapterStore),
                        sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
                        sanitizedQueryOptions.getAggregation(),
                        sanitizedQueryOptions.getFieldIdsAdapterPair(),
                        splitInfo.isMixedVisibility(),
                        splitInfo.isAuthorizationsLimiting(),
                        splitInfo.isClientsideRowMerging(),
                        r.getRange(),
                        sanitizedQueryOptions.getLimit(),
                        sanitizedQueryOptions.getMaxRangeDecomposition(),
                        sanitizedQueryOptions.getAuthorizations()))));
        incrementalRangeSums.put(r, sum);
        sum = sum.add(BigDecimal.valueOf(r.getCardinality()));
      }
      allIterators.add(
          concatenateWithCallback(
              indexReaders,
              callback,
              splitInfo.getIndex(),
              filters,
              dataIndexRetrieval));
    }
  }

  protected Iterator<Pair<GeoWaveInputKey, T>> rowReaderToKeyValues(
      final Index index,
      final QueryFilter[] filters,
      final DataIndexRetrieval dataIndexRetrieval,
      final Iterator<GeoWaveRow> reader) {
    InputFormatIteratorWrapper<T> iteratorWrapper;
    if (dataIndexRetrieval instanceof BatchDataIndexRetrieval) {
      // need special handling to account for asynchronous batched retrieval from the data index
      iteratorWrapper =
          new AsyncInputFormatIteratorWrapper<>(
              reader,
              filters,
              adapterStore,
              internalAdapterStore,
              aimStore,
              index,
              isOutputWritable,
              (BatchDataIndexRetrieval) dataIndexRetrieval);
    } else {
      iteratorWrapper =
          new InputFormatIteratorWrapper<>(
              reader,
              filters,
              adapterStore,
              internalAdapterStore,
              aimStore,
              index,
              isOutputWritable,
              dataIndexRetrieval);
    }
    return iteratorWrapper;
  }

  @Override
  public void close() {
    if (iterator != null) {
      iterator.close();
    }
  }

  @Override
  public GeoWaveInputKey getCurrentKey() throws IOException, InterruptedException {
    return currentGeoWaveKey;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (iterator != null) {
      if (iterator.hasNext()) {
        ++numKeysRead;
        final Object value = iterator.next();
        if (value instanceof Entry) {
          final Entry<GeoWaveInputKey, T> entry = (Entry<GeoWaveInputKey, T>) value;
          currentGeoWaveKey = entry.getKey();
          currentValue = entry.getValue();
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public T getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  protected static interface NextRangeCallback {
    public void setRange(RangeLocationPair indexPair);
  }

  /** Mostly guava's concatenate method, but there is a need for a callback between iterators */
  protected CloseableIterator<Pair<GeoWaveInputKey, T>> concatenateWithCallback(
      final List<Pair<RangeLocationPair, RowReader<GeoWaveRow>>> inputs,
      final NextRangeCallback nextRangeCallback,
      final Index index,
      final QueryFilter[] filters,
      final DataIndexRetrieval dataIndexRetrieval) {
    Preconditions.checkNotNull(inputs);
    return new CloseableIteratorWrapper<>(new Closeable() {
      @Override
      public void close() {
        for (final Pair<RangeLocationPair, RowReader<GeoWaveRow>> input : inputs) {
          input.getRight().close();
        }
      }
    },
        rowReaderToKeyValues(
            index,
            filters,
            dataIndexRetrieval,
            new ConcatenatedIteratorWithCallback(nextRangeCallback, inputs.iterator())));
  }

  private static float getOverallProgress(
      final GeoWaveRowRange range,
      final GeoWaveInputKey currentKey,
      final ProgressPerRange progress) {
    final float rangeProgress = getProgressForRange(range, currentKey);
    return progress.getOverallProgress(rangeProgress);
  }

  private static float getProgressForRange(
      final byte[] start,
      final byte[] end,
      final byte[] position) {
    final int maxDepth = Math.min(Math.max(end.length, start.length), position.length);
    final BigInteger startBI = new BigInteger(SplitsProvider.extractBytes(start, maxDepth));
    final BigInteger endBI = new BigInteger(SplitsProvider.extractBytes(end, maxDepth));
    final BigInteger positionBI = new BigInteger(SplitsProvider.extractBytes(position, maxDepth));
    return (float) (positionBI.subtract(startBI).doubleValue()
        / endBI.subtract(startBI).doubleValue());
  }

  private static float getProgressForRange(
      final GeoWaveRowRange range,
      final GeoWaveInputKey currentKey) {
    if (currentKey == null) {
      return 0f;
    }
    if ((range != null)
        && (range.getStartSortKey() != null)
        && (range.getEndSortKey() != null)
        && (currentKey.getGeoWaveKey() != null)) {
      // TODO GEOWAVE-1018 this doesn't account for partition keys at all
      // just look at the row progress
      return getProgressForRange(
          range.getStartSortKey(),
          range.getEndSortKey(),
          GeoWaveKey.getCompositeId(currentKey.getGeoWaveKey()));
    }
    // if we can't figure it out, then claim no progress
    return 0f;
  }

  @Override
  public float getProgress() throws IOException {
    if ((numKeysRead > 0) && (currentGeoWaveKey == null)) {
      return 1.0f;
    }
    if (currentGeoWaveRangeIndexPair == null) {
      return 0.0f;
    }
    final ProgressPerRange progress = progressPerRange.get(currentGeoWaveRangeIndexPair);
    if (progress == null) {
      return Math.min(
          1,
          Math.max(
              0,
              getProgressForRange(currentGeoWaveRangeIndexPair.getRange(), currentGeoWaveKey)));
    }
    return Math.min(
        1,
        Math.max(
            0,
            getOverallProgress(
                currentGeoWaveRangeIndexPair.getRange(),
                currentGeoWaveKey,
                progress)));
  }

  private class InternalCallback implements NextRangeCallback {

    @Override
    public void setRange(final RangeLocationPair indexPair) {
      currentGeoWaveRangeIndexPair = indexPair;
    }
  }

  private static class ConcatenatedIteratorWithCallback implements Iterator<GeoWaveRow> {
    private final NextRangeCallback nextRangeCallback;
    private final Iterator<Pair<RangeLocationPair, RowReader<GeoWaveRow>>> inputIteratorOfIterators;
    private Iterator<GeoWaveRow> currentIterator = Collections.emptyIterator();
    private Iterator<GeoWaveRow> removeFrom;

    public ConcatenatedIteratorWithCallback(
        final NextRangeCallback nextRangeCallback,
        final Iterator<Pair<RangeLocationPair, RowReader<GeoWaveRow>>> inputIteratorOfIterators) {
      super();
      this.nextRangeCallback = nextRangeCallback;
      this.inputIteratorOfIterators = inputIteratorOfIterators;
    }

    @Override
    public boolean hasNext() {
      boolean currentHasNext;
      while (!(currentHasNext = Preconditions.checkNotNull(currentIterator).hasNext())
          && inputIteratorOfIterators.hasNext()) {
        final Entry<RangeLocationPair, RowReader<GeoWaveRow>> entry =
            inputIteratorOfIterators.next();
        nextRangeCallback.setRange(entry.getKey());
        currentIterator = entry.getValue();
      }
      return currentHasNext;
    }

    @Override
    public GeoWaveRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      removeFrom = currentIterator;
      return currentIterator.next();
    }

    @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH", justification = "Precondition catches null")
    @Override
    public void remove() {
      Preconditions.checkState(
          removeFrom != null,
          "no calls to next() since last call to remove()");
      removeFrom.remove();
      removeFrom = null;
    }
  }
}
