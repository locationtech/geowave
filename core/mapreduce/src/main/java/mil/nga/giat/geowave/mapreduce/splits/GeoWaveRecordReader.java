package mil.nga.giat.geowave.mapreduce.splits;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
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
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

/**
 * This class is used by the GeoWaveInputFormat to read data from a GeoWave data
 * store.
 *
 * @param <T>
 *            The native type for the reader
 */
public abstract class GeoWaveRecordReader<T> extends
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
	protected QueryOptions queryOptions;
	protected boolean isOutputWritable;
	protected AdapterStore adapterStore;

	public GeoWaveRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final boolean isOutputWritable,
			final AdapterStore adapterStore ) {
		this.query = query;
		this.queryOptions = queryOptions;
		this.isOutputWritable = isOutputWritable;
		this.adapterStore = adapterStore;
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

		final Map<RangeLocationPair, CloseableIterator<?>> iteratorsPerRange = new LinkedHashMap<RangeLocationPair, CloseableIterator<?>>();

		final Set<PrimaryIndex> indices = split.getIndices();
		BigDecimal sum = BigDecimal.ZERO;

		final Map<RangeLocationPair, BigDecimal> incrementalRangeSums = new LinkedHashMap<RangeLocationPair, BigDecimal>();

		for (final PrimaryIndex i : indices) {
			final List<RangeLocationPair> ranges = split.getRanges(i);
			List<QueryFilter> queryFilters = null;
			if (query != null) {
				queryFilters = query.createFilters(i.getIndexModel());
			}
			for (final RangeLocationPair r : ranges) {
				final QueryOptions rangeQueryOptions = new QueryOptions(
						queryOptions);
				rangeQueryOptions.setIndex(i);
				iteratorsPerRange.put(
						r,
						queryRange(
								i,
								r.getRange(),
								queryFilters,
								rangeQueryOptions));
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
			catch (Exception e) {
				LOGGER.warn(
						"Unable to calculate progress",
						e);
			}
		}
		// concatenate iterators
		iterator = new CloseableIteratorWrapper<Object>(
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

	protected abstract CloseableIterator<?> queryRange(
			PrimaryIndex i,
			GeoWaveRowRange range,
			List<QueryFilter> queryFilters,
			QueryOptions rangeQueryOptions );

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
	protected static Iterator<Object> concatenateWithCallback(
			final Iterator<Entry<RangeLocationPair, CloseableIterator<?>>> inputs,
			final NextRangeCallback nextRangeCallback ) {
		Preconditions.checkNotNull(inputs);
		return new Iterator<Object>() {
			Iterator<?> currentIterator = Iterators.emptyIterator();
			Iterator<?> removeFrom;

			@Override
			public boolean hasNext() {
				boolean currentHasNext;
				while (!(currentHasNext = Preconditions.checkNotNull(
						currentIterator).hasNext()) && inputs.hasNext()) {
					final Entry<RangeLocationPair, CloseableIterator<?>> entry = inputs.next();
					nextRangeCallback.setRange(entry.getKey());
					currentIterator = entry.getValue();
				}
				return currentHasNext;
			}

			@Override
			public Object next() {
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
}