package mil.nga.giat.geowave.datastore.accumulo.mapreduce.input;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.query.InputFormatAccumuloRangeQuery;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

/**
 * This class is used by the GeoWaveInputFormat to read data from an Accumulo
 * data store.
 * 
 * @param <T>
 *            The native type for the reader
 */
public class GeoWaveRecordReader<T> extends
		RecordReader<GeoWaveInputKey, T>
{
	private static final class RangeIndexPair
	{
		private final Range range;
		private final Index index;

		public RangeIndexPair(
				final Range range,
				final Index index ) {
			this.range = range;
			this.index = index;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((index == null) ? 0 : index.hashCode());
			result = (prime * result) + ((range == null) ? 0 : range.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final RangeIndexPair other = (RangeIndexPair) obj;
			if (index == null) {
				if (other.index != null) {
					return false;
				}
			}
			else if (!index.equals(other.index)) {
				return false;
			}
			if (range == null) {
				if (other.range != null) {
					return false;
				}
			}
			else if (!range.equals(other.range)) {
				return false;
			}
			return true;
		}
	}

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

	protected static final Logger LOGGER = Logger.getLogger(GeoWaveRecordReader.class);
	protected long numKeysRead;
	protected CloseableIterator<?> iterator;
	protected Key currentAccumuloKey = null;
	protected Map<RangeIndexPair, ProgressPerRange> progressPerRange;
	protected GeoWaveInputKey currentGeoWaveKey = null;
	protected RangeIndexPair currentGeoWaveRangeIndexPair = null;
	protected T currentValue = null;
	protected GeoWaveInputSplit split;

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

		try {
			final AccumuloOperations operations = GeoWaveInputFormat.getAccumuloOperations(attempt);
			final boolean isOutputWritable = GeoWaveInputFormat.isOutputWritable(attempt);
			final JobContextAdapterStore adapterStore = GeoWaveInputFormat.getDataAdapterStore(
					attempt,
					operations);
			final DistributableQuery query = GeoWaveInputFormat.getQuery(attempt);
			final QueryOptions queryOptions = GeoWaveInputFormat.getQueryOptions(attempt);
			final String[] additionalAuthorizations = GeoWaveInputFormat.getAuthorizations(attempt);

			numKeysRead = 0;
			final Map<RangeIndexPair, CloseableIterator<?>> iteratorsPerRange = new LinkedHashMap<RangeIndexPair, CloseableIterator<?>>();

			final Set<Index> indices = split.getIndices();
			for (final Index i : indices) {
				final List<Range> ranges = split.getRanges(i);
				List<QueryFilter> queryFilters = null;
				if (query != null) {
					queryFilters = query.createFilters(i.getIndexModel());
				}
				for (final Range r : ranges) {
					iteratorsPerRange.put(
							new RangeIndexPair(
									r,
									i),
							new InputFormatAccumuloRangeQuery(
									GeoWaveInputFormat.getAdapterIds(
											attempt,
											adapterStore),
									i,
									r,
									queryFilters,
									isOutputWritable,
									queryOptions,
									additionalAuthorizations).query(
									operations,
									adapterStore,
									null,
									true));

				}
			}
			// calculate max cardinality
			final Collection<RangeIndexPair> pairs = iteratorsPerRange.keySet();
			int maxCardinality = 1;
			for (final RangeIndexPair p : pairs) {
				maxCardinality = Math.max(
						maxCardinality,
						GeoWaveInputFormat.getMaxCardinalityFromRange(p.range));
			}
			// calculate big integer sum of all ranges at this cardinality, and
			// keep track of the incremental sum at each range
			BigInteger sum = BigInteger.ZERO;
			final Map<RangeIndexPair, BigInteger> incrementalRangeSums = new LinkedHashMap<RangeIndexPair, BigInteger>();
			for (final RangeIndexPair p : pairs) {
				incrementalRangeSums.put(
						p,
						sum);
				final BigInteger range = GeoWaveInputFormat.getRange(
						p.range,
						maxCardinality);
				sum = sum.add(range);
			}
			// finally we can compute percent progress
			progressPerRange = new LinkedHashMap<RangeIndexPair, ProgressPerRange>();
			RangeIndexPair prevRangeIndex = null;
			float prevProgress = 0f;
			final BigDecimal decimalSum = new BigDecimal(
					sum);
			for (final Entry<RangeIndexPair, BigInteger> entry : incrementalRangeSums.entrySet()) {
				final BigInteger value = entry.getValue();
				final float progress = (float) new BigDecimal(
						value).divide(
						decimalSum,
						RoundingMode.HALF_UP).doubleValue();
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
										final RangeIndexPair indexPair ) {
									currentGeoWaveRangeIndexPair = indexPair;
								}
							}));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to query accumulo for range input split",
					e);
		}
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
					if (currentGeoWaveKey == null) {
						currentAccumuloKey = null;
					}
					else {
						currentAccumuloKey = currentGeoWaveKey.getAccumuloKey();
					}
					currentValue = entry.getValue();
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public float getProgress()
			throws IOException {
		if ((numKeysRead > 0) && (currentAccumuloKey == null)) {
			return 1.0f;
		}
		if (currentGeoWaveRangeIndexPair == null) {
			return 0.0f;
		}
		final ProgressPerRange progress = progressPerRange.get(currentGeoWaveRangeIndexPair);
		if (progress == null) {
			return getProgressForRange(
					currentGeoWaveRangeIndexPair.range,
					currentAccumuloKey);
		}
		return getOverallProgress(
				currentGeoWaveRangeIndexPair.range,
				currentAccumuloKey,
				progress);
	}

	private static float getOverallProgress(
			final Range range,
			final Key currentKey,
			final ProgressPerRange progress ) {
		final float rangeProgress = getProgressForRange(
				range,
				currentKey);
		return progress.getOverallProgress(rangeProgress);
	}

	private static float getProgressForRange(
			final ByteSequence start,
			final ByteSequence end,
			final ByteSequence position ) {
		final int maxDepth = Math.min(
				Math.max(
						end.length(),
						start.length()),
				position.length());
		final BigInteger startBI = new BigInteger(
				GeoWaveInputFormat.extractBytes(
						start,
						maxDepth));
		final BigInteger endBI = new BigInteger(
				GeoWaveInputFormat.extractBytes(
						end,
						maxDepth));
		final BigInteger positionBI = new BigInteger(
				GeoWaveInputFormat.extractBytes(
						position,
						maxDepth));
		return (float) (positionBI.subtract(
				startBI).doubleValue() / endBI.subtract(
				startBI).doubleValue());
	}

	private static float getProgressForRange(
			final Range range,
			final Key currentKey ) {
		if (currentKey == null) {
			return 0f;
		}
		if ((range.getStartKey() != null) && (range.getEndKey() != null)) {
			if (!range.getStartKey().equals(
					range.getEndKey(),
					PartialKey.ROW)) {
				// just look at the row progress
				return getProgressForRange(
						range.getStartKey().getRowData(),
						range.getEndKey().getRowData(),
						currentKey.getRowData());
			}
			else if (!range.getStartKey().equals(
					range.getEndKey(),
					PartialKey.ROW_COLFAM)) {
				// just look at the column family progress
				return getProgressForRange(
						range.getStartKey().getColumnFamilyData(),
						range.getEndKey().getColumnFamilyData(),
						currentKey.getColumnFamilyData());
			}
			else if (!range.getStartKey().equals(
					range.getEndKey(),
					PartialKey.ROW_COLFAM_COLQUAL)) {
				// just look at the column qualifier progress
				return getProgressForRange(
						range.getStartKey().getColumnQualifierData(),
						range.getEndKey().getColumnQualifierData(),
						currentKey.getColumnQualifierData());
			}
		}
		// if we can't figure it out, then claim no progress
		return 0f;
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

	private static interface NextRangeCallback
	{
		public void setRange(
				RangeIndexPair indexPair );
	}

	/**
	 * Mostly guava's concatenate method, but there is a need for a callback
	 * between iterators
	 */
	private static Iterator<Object> concatenateWithCallback(
			final Iterator<Entry<RangeIndexPair, CloseableIterator<?>>> inputs,
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
					final Entry<RangeIndexPair, CloseableIterator<?>> entry = inputs.next();
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
