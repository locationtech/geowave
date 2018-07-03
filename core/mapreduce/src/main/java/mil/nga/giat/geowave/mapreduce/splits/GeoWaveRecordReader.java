/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.mapreduce.splits;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreWrapper;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.BaseQueryOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderClosableWrapper;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStoreOperations;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.input.InputFormatIteratorWrapper;

/**
 * This class is used by the GeoWaveInputFormat to read data from a GeoWave data
 * store.
 *
 * @param <T>
 *            The native type for the reader
 */
public class GeoWaveRecordReader<T> extends
		RecordReader<GeoWaveInputKey, T>
{

	protected static class ProgressPerRange
	{
		private final float startProgress;
		private final float deltaProgress;

		public ProgressPerRange(
				final float startProgress,
				final float endProgress ) {
			this.startProgress = startProgress;
			this.deltaProgress = endProgress - startProgress;
		}

		public float getOverallProgress(
				final float rangeProgress ) {
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
	protected DistributableQuery query;
	protected BaseQueryOptions sanitizedQueryOptions;
	protected boolean isOutputWritable;
	protected TransientAdapterStore adapterStore;
	protected InternalAdapterStore internalAdapterStore;
	protected AdapterIndexMappingStore aimStore;
	protected IndexStore indexStore;
	protected BaseDataStore dataStore;
	protected MapReduceDataStoreOperations operations;

	public GeoWaveRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final boolean isOutputWritable,
			final TransientAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore aimStore,
			final IndexStore indexStore,
			final MapReduceDataStoreOperations operations ) {
		this.query = query;
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		sanitizedQueryOptions = new BaseQueryOptions(
				(queryOptions == null) ? new QueryOptions() : queryOptions,
				internalAdapterStore);
		this.isOutputWritable = isOutputWritable;
		this.adapterStore = adapterStore;
		this.internalAdapterStore = internalAdapterStore;
		this.aimStore = aimStore;
		this.indexStore = indexStore;
		this.operations = operations;
	}

	/**
	 * Initialize a scanner over the given input split using this task attempt
	 * configuration.
	 */
	@Override
	public void initialize(
			final InputSplit inSplit,
			final TaskAttemptContext attempt )
			throws IOException {
		split = (GeoWaveInputSplit) inSplit;

		numKeysRead = 0;

		final Map<RangeLocationPair, CloseableIterator<Entry<GeoWaveInputKey, T>>> iteratorsPerRange = new LinkedHashMap<RangeLocationPair, CloseableIterator<Entry<GeoWaveInputKey, T>>>();

		final Set<ByteArrayId> indices = split.getIndexIds();
		BigDecimal sum = BigDecimal.ZERO;

		final Map<RangeLocationPair, BigDecimal> incrementalRangeSums = new LinkedHashMap<RangeLocationPair, BigDecimal>();

		for (final ByteArrayId i : indices) {
			final SplitInfo splitInfo = split.getInfo(i);
			List<QueryFilter> queryFilters = null;
			if (query != null) {
				queryFilters = query.createFilters(splitInfo.getIndex());
			}
			for (final RangeLocationPair r : splitInfo.getRangeLocationPairs()) {
				iteratorsPerRange.put(
						r,
						queryRange(
								splitInfo.getIndex(),
								r.getRange(),
								queryFilters,
								splitInfo.isMixedVisibility()));
				incrementalRangeSums.put(
						r,
						sum);
				sum = sum.add(BigDecimal.valueOf(r.getCardinality()));
			}
		}

		// finally we can compute percent progress
		progressPerRange = new LinkedHashMap<RangeLocationPair, ProgressPerRange>();
		RangeLocationPair prevRangeIndex = null;
		float prevProgress = 0f;
		if (sum.compareTo(BigDecimal.ZERO) > 0) {
			try {
				for (final Entry<RangeLocationPair, BigDecimal> entry : incrementalRangeSums.entrySet()) {
					final BigDecimal value = entry.getValue();
					final float progress = value.divide(
							sum,
							RoundingMode.HALF_UP).floatValue();
					if (prevRangeIndex != null) {
						progressPerRange.put(
								prevRangeIndex,
								new ProgressPerRange(
										prevProgress,
										progress));
					}
					prevRangeIndex = entry.getKey();
					prevProgress = progress;
				}
				progressPerRange.put(
						prevRangeIndex,
						new ProgressPerRange(
								prevProgress,
								1f));

			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to calculate progress",
						e);
			}
		}
		// concatenate iterators
		iterator = new CloseableIteratorWrapper<Entry<GeoWaveInputKey, T>>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<?> it : iteratorsPerRange.values()) {
							it.close();
						}
					}
				},
				concatenateWithCallback(
						iteratorsPerRange.entrySet().iterator(),
						new NextRangeCallback() {

							@Override
							public void setRange(
									final RangeLocationPair indexPair ) {
								currentGeoWaveRangeIndexPair = indexPair;
							}
						}));

	}

	protected CloseableIterator<Entry<GeoWaveInputKey, T>> queryRange(
			final PrimaryIndex index,
			final GeoWaveRowRange range,
			final List<QueryFilter> queryFilters,
			final boolean mixedVisibility ) {

		final QueryFilter singleFilter = ((queryFilters == null) || queryFilters.isEmpty()) ? null : queryFilters
				.size() == 1 ? queryFilters.get(0) : new FilterList<QueryFilter>(
				queryFilters);
		final Reader reader = operations.createReader(new RecordReaderParams(
				index,
				new AdapterStoreWrapper(
						adapterStore,
						internalAdapterStore),
				sanitizedQueryOptions.getAdapterIds(),
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getAggregation(),
				sanitizedQueryOptions.getFieldIdsAdapterPair(),
				mixedVisibility,
				range,
				sanitizedQueryOptions.getLimit(),
				GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
				sanitizedQueryOptions.getAuthorizations()));
		return new CloseableIteratorWrapper(
				new ReaderClosableWrapper(
						reader),
				new InputFormatIteratorWrapper<>(
						reader,
						singleFilter,
						adapterStore,
						internalAdapterStore,
						index,
						isOutputWritable));
	}

	@Override
	public void close() {
		if (iterator != null) {
			try {
				iterator.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close iterator",
						e);
			}
		}
	}

	@Override
	public GeoWaveInputKey getCurrentKey()
			throws IOException,
			InterruptedException {
		return currentGeoWaveKey;
	}

	@Override
	public boolean nextKeyValue()
			throws IOException,
			InterruptedException {
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
	public T getCurrentValue()
			throws IOException,
			InterruptedException {
		return currentValue;
	}

	protected static interface NextRangeCallback
	{
		public void setRange(
				RangeLocationPair indexPair );
	}

	/**
	 * Mostly guava's concatenate method, but there is a need for a callback
	 * between iterators
	 */
	protected static <T> Iterator<Entry<GeoWaveInputKey, T>> concatenateWithCallback(
			final Iterator<Entry<RangeLocationPair, CloseableIterator<Entry<GeoWaveInputKey, T>>>> inputs,
			final NextRangeCallback nextRangeCallback ) {
		Preconditions.checkNotNull(inputs);
		return new Iterator<Entry<GeoWaveInputKey, T>>() {
			Iterator<Entry<GeoWaveInputKey, T>> currentIterator = Iterators.emptyIterator();
			Iterator<Entry<GeoWaveInputKey, T>> removeFrom;

			@Override
			public boolean hasNext() {
				boolean currentHasNext;
				while (!(currentHasNext = Preconditions.checkNotNull(
						currentIterator).hasNext()) && inputs.hasNext()) {
					final Entry<RangeLocationPair, CloseableIterator<Entry<GeoWaveInputKey, T>>> entry = inputs.next();
					nextRangeCallback.setRange(entry.getKey());
					currentIterator = entry.getValue();
				}
				return currentHasNext;
			}

			@Override
			public Entry<GeoWaveInputKey, T> next() {
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
		};
	}

	private static float getOverallProgress(
			final GeoWaveRowRange range,
			final GeoWaveInputKey currentKey,
			final ProgressPerRange progress ) {
		final float rangeProgress = getProgressForRange(
				range,
				currentKey);
		return progress.getOverallProgress(rangeProgress);
	}

	private static float getProgressForRange(
			final byte[] start,
			final byte[] end,
			final byte[] position ) {
		final int maxDepth = Math.min(
				Math.max(
						end.length,
						start.length),
				position.length);
		final BigInteger startBI = new BigInteger(
				SplitsProvider.extractBytes(
						start,
						maxDepth));
		final BigInteger endBI = new BigInteger(
				SplitsProvider.extractBytes(
						end,
						maxDepth));
		final BigInteger positionBI = new BigInteger(
				SplitsProvider.extractBytes(
						position,
						maxDepth));
		return (float) (positionBI.subtract(
				startBI).doubleValue() / endBI.subtract(
				startBI).doubleValue());
	}

	private static float getProgressForRange(
			final GeoWaveRowRange range,
			final GeoWaveInputKey currentKey ) {
		if (currentKey == null) {
			return 0f;
		}
		if ((range != null) && (range.getStartSortKey() != null) && (range.getEndSortKey() != null)
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
	public float getProgress()
			throws IOException {
		if ((numKeysRead > 0) && (currentGeoWaveKey == null)) {
			return 1.0f;
		}
		if (currentGeoWaveRangeIndexPair == null) {
			return 0.0f;
		}
		final ProgressPerRange progress = progressPerRange.get(currentGeoWaveRangeIndexPair);
		if (progress == null) {
			return getProgressForRange(
					currentGeoWaveRangeIndexPair.getRange(),
					currentGeoWaveKey);
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

}