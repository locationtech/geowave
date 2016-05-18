package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

public class AggregationIterator extends
		QueryFilterIterator
{
	private static final Logger LOGGER = Logger.getLogger(AggregationIterator.class);
	public static final String AGGREGATION_QUERY_ITERATOR_NAME = "GEOWAVE_AGGREGATION_ITERATOR";
	public static final String AGGREGATION_OPTION_NAME = "AGGREGATION";
	public static final String PARAMETER_OPTION_NAME = "PARAMETER";
	public static final String ADAPTER_OPTION_NAME = "ADAPTER";
	public static final String INDEX_STRATEGY_OPTION_NAME = "INDEX_STRATEGY";
	public static final String CONSTRAINTS_OPTION_NAME = "CONSTRAINTS";
	public static final String MAX_DECOMPOSITION_OPTION_NAME = "MAX_DECOMP";
	public static final int STATISTICS_QUERY_ITERATOR_PRIORITY = 10;
	private Aggregation aggregationFunction;
	private DataAdapter adapter;
	private boolean aggregationReturned = false;
	private Text startRowOfAggregation = null;
	private TreeSet<Range> ranges;

	public AggregationIterator() {
		super();
	}

	@Override
	public Key getTopKey() {
		if (hasTopOriginal()) {
			return getTopOriginalKey();
		}
		else if (hasTopStat()) {
			return getTopStatKey();
		}
		return null;
	}

	@Override
	public Value getTopValue() {
		if (hasTopOriginal()) {
			return getTopOriginalValue();
		}
		else if (hasTopStat()) {
			return getTopStatValue();
		}
		return null;
	}

	@Override
	public boolean hasTop() {
		// firstly iterate through all of the original data values
		final boolean hasTopOriginal = hasTopOriginal();
		if (hasTopOriginal) {
			return true;
		}
		return hasTopStat();
	}

	@Override
	protected boolean accept(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values,
			final CommonIndexModel model,
			final CommonIndexedPersistenceEncoding persistenceEncoding ) {
		if (super.accept(
				currentRow,
				keys,
				values,
				model,
				persistenceEncoding)) {
			if (persistenceEncoding.getAdapterId().getString().equals(
					adapter.getAdapterId().getString())) {
				final PersistentDataset<Object> adapterExtendedValues = new PersistentDataset<Object>();
				if (persistenceEncoding instanceof AbstractAdapterPersistenceEncoding) {
					((AbstractAdapterPersistenceEncoding) persistenceEncoding).convertUnknownValues(
							adapter,
							model);
					final PersistentDataset<Object> existingExtValues = ((AbstractAdapterPersistenceEncoding) persistenceEncoding)
							.getAdapterExtendedData();
					if (existingExtValues != null) {
						for (final PersistentValue<Object> val : existingExtValues.getValues()) {
							adapterExtendedValues.addValue(val);
						}
					}
				}
				final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
						persistenceEncoding.getAdapterId(),
						persistenceEncoding.getDataId(),
						persistenceEncoding.getIndexInsertionId(),
						persistenceEncoding.getDuplicateCount(),
						persistenceEncoding.getCommonData(),
						new PersistentDataset<byte[]>(),
						adapterExtendedValues);
				final Object row = adapter.decode(
						encoding,
						new PrimaryIndex(
								null, // the data adapter can't use the numeric
										// index
										// strategy and only the common index
										// model to decode which is the case for
										// feature data,
										// we pass along a null strategy to
										// eliminate the necessity to send a
										// serialization of the strategy in the
										// options of this iterator
								model));
				if (row != null) {
					// for now ignore field info
					aggregationFunction.aggregate(row);
					if (startRowOfAggregation == null) {
						startRowOfAggregation = currentRow;
					}
				}
			}
		}
		return false;
	}

	@Override
	public void next()
			throws IOException {
		if (super.hasTop()) {
			super.next();
		}
		else {
			// there's only one instance of stat that we want to return
			// return it and finish
			aggregationReturned = true;
		}
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		try {

			final String className = options.get(AGGREGATION_OPTION_NAME);
			aggregationFunction = PersistenceUtils.classFactory(
					className,
					Aggregation.class);
			final String parameterStr = options.get(PARAMETER_OPTION_NAME);
			if ((parameterStr != null) && parameterStr.isEmpty()) {
				final byte[] parameterBytes = ByteArrayUtils.byteArrayFromString(parameterStr);
				final Persistable aggregationParams = PersistenceUtils.fromBinary(
						parameterBytes,
						Persistable.class);
				aggregationFunction.setParameters(aggregationParams);
			}
			final String adapterStr = options.get(ADAPTER_OPTION_NAME);
			final byte[] adapterBytes = ByteArrayUtils.byteArrayFromString(adapterStr);
			adapter = PersistenceUtils.fromBinary(
					adapterBytes,
					DataAdapter.class);

			// now go from index strategy, constraints, and max decomp to a set
			// of accumulo ranges

			final String indexStrategyStr = options.get(INDEX_STRATEGY_OPTION_NAME);
			final byte[] indexStrategyBytes = ByteArrayUtils.byteArrayFromString(indexStrategyStr);
			final NumericIndexStrategy strategy = PersistenceUtils.fromBinary(
					indexStrategyBytes,
					NumericIndexStrategy.class);

			final String contraintsStr = options.get(CONSTRAINTS_OPTION_NAME);
			final byte[] constraintsBytes = ByteArrayUtils.byteArrayFromString(contraintsStr);
			final List constraints = PersistenceUtils.fromBinary(constraintsBytes);
			final String maxDecomp = options.get(MAX_DECOMPOSITION_OPTION_NAME);
			Integer maxDecompInt = AccumuloConstraintsQuery.MAX_RANGE_DECOMPOSITION;
			if (maxDecomp != null) {
				try {
					maxDecompInt = Integer.parseInt(maxDecomp);
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to parse '" + MAX_DECOMPOSITION_OPTION_NAME + "' as integer",
							e);
				}
			}
			ranges = AccumuloUtils.byteArrayRangesToAccumuloRanges(DataStoreUtils.constraintsToByteArrayRanges(
					constraints,
					strategy,
					maxDecompInt));
			super.init(
					source,
					options,
					env);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}

	private Key getTopOriginalKey() {
		return super.getTopKey();
	}

	private Value getTopOriginalValue() {
		return super.getTopValue();
	}

	private boolean hasTopOriginal() {
		return super.hasTop();
	}

	private Key getTopStatKey() {
		if (hasTopStat()) {
			return new Key(
					startRowOfAggregation);
		}
		return null;
	}

	private Value getTopStatValue() {
		if (hasTopStat()) {

			final Mergeable result = aggregationFunction.getResult();
			if (result == null) {
				return null;
			}
			return new Value(
					PersistenceUtils.toBinary(result));
		}
		return null;
	}

	private boolean hasTopStat() {
		return !aggregationReturned && (startRowOfAggregation != null);
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final SortedKeyValueIterator<Key, Value> iterator = super.deepCopy(env);
		if (iterator instanceof AggregationIterator) {
			((AggregationIterator) iterator).startRowOfAggregation = startRowOfAggregation;
			((AggregationIterator) iterator).adapter = adapter;
			((AggregationIterator) iterator).aggregationFunction = aggregationFunction;
			((AggregationIterator) iterator).aggregationReturned = aggregationReturned;
		}
		return iterator;
	}

	private void findEnd(
			final Iterator<Range> rangeIt,
			final Collection<Range> internalRanges,
			final Range seekRange ) {
		// find the first range in the set whose end key is after this
		// range's end key, clip its end to this range end if its start
		// is not also greater than this end, and stop
		// after that
		while (rangeIt.hasNext()) {
			final Range internalRange = rangeIt.next();
			if (internalRange.getEndKey() == null || internalRange.getEndKey().compareTo(
					seekRange.getEndKey()) > 0) {
				if (internalRange.getStartKey() != null && internalRange.getStartKey().compareTo(
						seekRange.getEndKey()) > 0) {
					return;
				}
				else {
					internalRanges.add(new Range(
							internalRange.getStartKey(),
							seekRange.getEndKey()));
					return;
				}
			}
			else {
				internalRanges.add(internalRange);
			}
		}
	}

	private void findStart(
			final Iterator<Range> rangeIt,
			final Collection<Range> internalRanges,
			final Range seekRange ) {
		// find the first range whose end key is after this range's start key
		// and clip its start to this range start key, and start on that
		while (rangeIt.hasNext()) {
			final Range internalRange = rangeIt.next();
			if (internalRange.getEndKey() == null || internalRange.getEndKey().compareTo(
					seekRange.getStartKey()) > 0) {
				if (internalRange.getStartKey() != null && internalRange.getStartKey().compareTo(
						seekRange.getStartKey()) > 0) {
					internalRanges.add(internalRange);
					return;
				}
				else {
					internalRanges.add(new Range(
							seekRange.getStartKey(),
							internalRange.getEndKey()));
					return;
				}
			}
		}
	}

	@Override
	public void seek(
			final Range seekRange,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive )
			throws IOException {
		aggregationReturned = false;
		aggregationFunction.clearResult();
		startRowOfAggregation = null;
		Collection<Range> internalRanges = new ArrayList<Range>();
		if (seekRange.isInfiniteStartKey()) {
			if (seekRange.isInfiniteStopKey()) {
				internalRanges = ranges;
			}
			else {
				findEnd(
						ranges.iterator(),
						internalRanges,
						seekRange);
			}
		}
		else if (seekRange.isInfiniteStopKey()) {
			final Iterator<Range> rangeIt = ranges.iterator();
			findStart(
					rangeIt,
					internalRanges,
					seekRange);
			while (rangeIt.hasNext()) {
				internalRanges.add(rangeIt.next());
			}
		}
		else {
			final Iterator<Range> rangeIt = ranges.iterator();
			findStart(
					rangeIt,
					internalRanges,
					seekRange);
			findEnd(
					rangeIt,
					internalRanges,
					seekRange);
		}
		final Iterator<Range> rangeIt = internalRanges.iterator();
		while (rangeIt.hasNext()) {
			super.seek(
					rangeIt.next(),
					columnFamilies,
					inclusive);
		}
	}
}