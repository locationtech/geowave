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
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.ConstraintsQuery;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

public class AggregationIterator extends
		Filter
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationIterator.class);
	public static final String AGGREGATION_QUERY_ITERATOR_NAME = "GEOWAVE_AGGREGATION_ITERATOR";
	public static final String AGGREGATION_OPTION_NAME = "AGGREGATION";
	public static final String PARAMETER_OPTION_NAME = "PARAMETER";
	public static final String ADAPTER_OPTION_NAME = "ADAPTER";
	public static final String INDEX_STRATEGY_OPTION_NAME = "INDEX_STRATEGY";
	public static final String CONSTRAINTS_OPTION_NAME = "CONSTRAINTS";
	public static final String MAX_DECOMPOSITION_OPTION_NAME = "MAX_DECOMP";
	public static final int AGGREGATION_QUERY_ITERATOR_PRIORITY = 25;
	protected QueryFilterIterator queryFilterIterator;
	private Aggregation aggregationFunction;
	private DataAdapter adapter;
	private boolean aggregationReturned = false;
	private Text startRowOfAggregation = null;
	private Text currentRow = new Text();
	private SortedKeyValueIterator<Key, Value> parent = new SortedKeyValueIterator<Key, Value>() {

		@Override
		public void init(
				final SortedKeyValueIterator<Key, Value> source,
				final Map<String, String> options,
				final IteratorEnvironment env )
				throws IOException {
			AggregationIterator.super.init(
					source,
					options,
					env);
		}

		@Override
		public boolean hasTop() {
			return AggregationIterator.super.hasTop();
		}

		@Override
		public void next()
				throws IOException {
			AggregationIterator.super.next();
		}

		@Override
		public void seek(
				final Range range,
				final Collection<ByteSequence> columnFamilies,
				final boolean inclusive )
				throws IOException {
			AggregationIterator.super.seek(
					range,
					columnFamilies,
					inclusive);
		}

		@Override
		public Key getTopKey() {
			return AggregationIterator.super.getTopKey();
		}

		@Override
		public Value getTopValue() {
			return AggregationIterator.super.getTopValue();
		}

		@Override
		public SortedKeyValueIterator<Key, Value> deepCopy(
				final IteratorEnvironment env ) {
			return AggregationIterator.super.deepCopy(env);
		}
	};
	private TreeSet<Range> ranges;

	@Override
	public boolean accept(
			final Key key,
			final Value value ) {
		if (queryFilterIterator != null) {
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			key.getRow(currentRow);
			final FlattenedUnreadData unreadData = queryFilterIterator.aggregateFieldData(
					key,
					value,
					commonData);
			final CommonIndexedPersistenceEncoding encoding = QueryFilterIterator.getEncoding(
					currentRow,
					commonData,
					unreadData);

			boolean queryFilterResult = true;
			if (queryFilterIterator.isSet()) {
				queryFilterResult = queryFilterIterator.applyRowFilter(encoding);
			}
			if (queryFilterResult) {
				aggregateRow(
						currentRow,
						queryFilterIterator.model,
						encoding);
			}
		}
		// we don't want to return anything but the aggregation result
		return false;
	}

	public void setParent(
			final SortedKeyValueIterator<Key, Value> parent ) {
		this.parent = parent;
	}

	protected void aggregateRow(
			final Text currentRow,
			final CommonIndexModel model,
			final CommonIndexedPersistenceEncoding persistenceEncoding ) {
		if (adapter == null) {
			aggregationFunction.aggregate(persistenceEncoding);
			if (startRowOfAggregation == null) {
				startRowOfAggregation = currentRow;
			}
		}
		else if (persistenceEncoding.getAdapterId().getString().equals(
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
			// the data adapter can't use the numeric index strategy and only
			// the common index model to decode which is the case for feature
			// data, we pass along a null strategy to eliminate the necessity to
			// send a serialization of the strategy in the options of this
			// iterator
			final Object row = adapter.decode(
					encoding,
					new PrimaryIndex(
							null,

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

	public void setOptions(
			final Map<String, String> options ) {
		try {
			final String className = options.get(AGGREGATION_OPTION_NAME);
			aggregationFunction = PersistenceUtils.classFactory(
					className,
					Aggregation.class);
			final String parameterStr = options.get(PARAMETER_OPTION_NAME);
			if ((parameterStr != null) && !parameterStr.isEmpty()) {
				final byte[] parameterBytes = ByteArrayUtils.byteArrayFromString(parameterStr);
				final Persistable aggregationParams = PersistenceUtils.fromBinary(
						parameterBytes,
						Persistable.class);
				aggregationFunction.setParameters(aggregationParams);
			}
			if (options.containsKey(ADAPTER_OPTION_NAME)) {
				final String adapterStr = options.get(ADAPTER_OPTION_NAME);
				final byte[] adapterBytes = ByteArrayUtils.byteArrayFromString(adapterStr);
				adapter = PersistenceUtils.fromBinary(
						adapterBytes,
						DataAdapter.class);
			}

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
			Integer maxDecompInt = ConstraintsQuery.MAX_RANGE_DECOMPOSITION;
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
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
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
	public void next()
			throws IOException {
		if (parent.hasTop()) {
			parent.next();
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
		setOptions(options);
		queryFilterIterator = new QueryFilterIterator();
		queryFilterIterator.setOptions(options);
		parent.init(
				source,
				options,
				env);
	}

	protected Key getTopOriginalKey() {
		return parent.getTopKey();
	}

	protected Value getTopOriginalValue() {
		return parent.getTopValue();
	}

	protected boolean hasTopOriginal() {
		return parent.hasTop();
	}

	protected Key getTopStatKey() {
		if (hasTopStat()) {
			return new Key(
					startRowOfAggregation);
		}
		return null;
	}

	protected Value getTopStatValue() {
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

	protected boolean hasTopStat() {
		return !aggregationReturned && (startRowOfAggregation != null);
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final SortedKeyValueIterator<Key, Value> iterator = parent.deepCopy(env);
		deepCopyIterator(iterator);
		return iterator;
	}

	public void deepCopyIterator(
			final SortedKeyValueIterator<Key, Value> iterator ) {
		if (iterator instanceof AggregationIterator) {
			((AggregationIterator) iterator).startRowOfAggregation = startRowOfAggregation;
			((AggregationIterator) iterator).adapter = adapter;
			((AggregationIterator) iterator).queryFilterIterator = queryFilterIterator;
			((AggregationIterator) iterator).parent = parent;
			((AggregationIterator) iterator).aggregationFunction = aggregationFunction;
			((AggregationIterator) iterator).aggregationReturned = aggregationReturned;
		}
	}

	@Override
	protected void findTop() {
		QueryFilterIterator.findTopEnhanced(
				getSource(),
				this);
	}

	protected static void findEnd(
			final Iterator<Range> rangeIt,
			final Collection<Range> internalRanges,
			final Range seekRange ) {
		// find the first range in the set whose end key is after this
		// range's end key, clip its end to this range end if its start
		// is not also greater than this end, and stop
		// after that
		while (rangeIt.hasNext()) {
			final Range internalRange = rangeIt.next();
			if ((internalRange.getEndKey() == null) || (internalRange.getEndKey().compareTo(
					seekRange.getEndKey()) > 0)) {
				if ((internalRange.getStartKey() != null) && (internalRange.getStartKey().compareTo(
						seekRange.getEndKey()) > 0)) {
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

	protected static void findStart(
			final Iterator<Range> rangeIt,
			final Collection<Range> internalRanges,
			final Range seekRange ) {
		// find the first range whose end key is after this range's start key
		// and clip its start to this range start key, and start on that
		while (rangeIt.hasNext()) {
			final Range internalRange = rangeIt.next();
			if ((internalRange.getEndKey() == null) || (internalRange.getEndKey().compareTo(
					seekRange.getStartKey()) > 0)) {
				if ((internalRange.getStartKey() != null) && (internalRange.getStartKey().compareTo(
						seekRange.getStartKey()) > 0)) {
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
			parent.seek(
					rangeIt.next(),
					columnFamilies,
					inclusive);
		}
	}
}
