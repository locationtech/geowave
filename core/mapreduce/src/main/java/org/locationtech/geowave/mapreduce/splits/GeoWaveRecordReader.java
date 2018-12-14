/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.mapreduce.splits;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Collections;
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
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterStoreWrapper;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.base.BaseQueryOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.ReaderClosableWrapper;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.input.InputFormatIteratorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
			deltaProgress = endProgress - startProgress;
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
	protected QueryConstraints constraints;
	protected BaseQueryOptions sanitizedQueryOptions;
	protected boolean isOutputWritable;
	protected TransientAdapterStore adapterStore;
	protected InternalAdapterStore internalAdapterStore;
	protected AdapterIndexMappingStore aimStore;
	protected IndexStore indexStore;
	protected BaseDataStore dataStore;
	protected MapReduceDataStoreOperations operations;

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
			final MapReduceDataStoreOperations operations ) {
		this.constraints = constraints;
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		sanitizedQueryOptions = new BaseQueryOptions(
				commonOptions,
				typeOptions,
				indexOptions,
				new AdapterStoreWrapper(
						adapterStore,
						internalAdapterStore),
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

		final Map<RangeLocationPair, CloseableIterator<Entry<GeoWaveInputKey, T>>> iteratorsPerRange = new LinkedHashMap<>();

		final Set<String> indices = split.getIndexNames();
		BigDecimal sum = BigDecimal.ZERO;

		final Map<RangeLocationPair, BigDecimal> incrementalRangeSums = new LinkedHashMap<>();

		for (final String i : indices) {
			final SplitInfo splitInfo = split.getInfo(i);
			List<QueryFilter> queryFilters = null;
			if (constraints != null) {
				// do a check for AdapterAndIndexBasedQueryConstraints in case
				// the splits provider was unable to set it
				if (constraints instanceof AdapterAndIndexBasedQueryConstraints) {
					final short[] adapters = sanitizedQueryOptions.getAdapterIds(internalAdapterStore);
					DataTypeAdapter<?> adapter = null;
					// in practice this is used for CQL and you can't have
					// multiple
					// types/adapters
					if (adapters.length == 1) {
						final String typeName = internalAdapterStore.getTypeName(adapters[0]);
						if (typeName != null) {
							adapter = adapterStore.getAdapter(typeName);
						}
					}
					if (adapter == null) {
						LOGGER.warn("Unable to find type matching an adapter dependent query");
					}
					this.constraints = ((AdapterAndIndexBasedQueryConstraints) constraints).createQueryConstraints(
							adapter,
							splitInfo.getIndex());
				}

				queryFilters = constraints.createFilters(splitInfo.getIndex());
			}
			for (final RangeLocationPair r : splitInfo.getRangeLocationPairs()) {
				iteratorsPerRange.put(
						r,
						queryRange(
								splitInfo.getIndex(),
								r.getRange(),
								queryFilters,
								splitInfo.isMixedVisibility(),
								splitInfo.isAuthorizationsLimiting()));
				incrementalRangeSums.put(
						r,
						sum);
				sum = sum.add(BigDecimal.valueOf(r.getCardinality()));
			}
		}

		// finally we can compute percent progress
		progressPerRange = new LinkedHashMap<>();
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
		iterator = new CloseableIteratorWrapper<>(
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
			final Index index,
			final GeoWaveRowRange range,
			final List<QueryFilter> queryFilters,
			final boolean mixedVisibility,
			final boolean authorizationsLimiting ) {

		final QueryFilter singleFilter = ((queryFilters == null) || queryFilters.isEmpty()) ? null : queryFilters
				.size() == 1 ? queryFilters.get(0) : new FilterList(
				queryFilters);
		final RowReader reader = operations.createReader(new RecordReaderParams(
				index,
				new AdapterStoreWrapper(
						adapterStore,
						internalAdapterStore),
				internalAdapterStore,
				sanitizedQueryOptions.getAdapterIds(internalAdapterStore),
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getAggregation(),
				sanitizedQueryOptions.getFieldIdsAdapterPair(),
				mixedVisibility,
				authorizationsLimiting,
				range,
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getMaxRangeDecomposition(),
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
			iterator.close();
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
			Iterator<Entry<GeoWaveInputKey, T>> currentIterator = Collections.emptyIterator();
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
