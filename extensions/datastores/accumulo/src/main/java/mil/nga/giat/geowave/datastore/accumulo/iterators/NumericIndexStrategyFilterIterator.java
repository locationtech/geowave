package mil.nga.giat.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeUtils.RangeCache;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeUtils.RangeLookupFactory;

public class NumericIndexStrategyFilterIterator implements
		SortedKeyValueIterator<Key, Value>
{

	// this is after the versioning iterator at 20 but before the more expensive
	// distributable filter iterator at 25
	public static final int IDX_FILTER_ITERATOR_PRIORITY = 22;
	public static final String IDX_FILTER_ITERATOR_NAME = "GEOWAVE_IDX_FILTER";
	public static String COORDINATE_RANGE_KEY = "COORD_RANGE";
	public static String INDEX_STRATEGY_KEY = "IDX_STRATEGY";
	private SortedKeyValueIterator<Key, Value> source = null;
	private Key topKey = null;
	private Value topValue = null;
	private final Text row = new Text();
	private NumericIndexStrategy indexStrategy;
	private RangeCache rangeCache;
	private int partitionKeyLength = 0;

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		this.source = source;
		if (options == null) {
			throw new IllegalArgumentException(
					"Arguments must be set for " + NumericIndexStrategyFilterIterator.class.getName());
		}
		try {
			if (options.containsKey(INDEX_STRATEGY_KEY)) {
				final String idxStrategyStr = options.get(INDEX_STRATEGY_KEY);
				final byte[] idxStrategyBytes = ByteArrayUtils.byteArrayFromString(idxStrategyStr);
				indexStrategy = PersistenceUtils.fromBinary(
						idxStrategyBytes,
						NumericIndexStrategy.class);
				partitionKeyLength = indexStrategy.getPartitionKeyLength();
			}
			else {
				throw new IllegalArgumentException(
						"'" + INDEX_STRATEGY_KEY + "' must be set for "
								+ NumericIndexStrategyFilterIterator.class.getName());
			}
			if (options.containsKey(COORDINATE_RANGE_KEY)) {
				final String coordRangeStr = options.get(COORDINATE_RANGE_KEY);
				final byte[] coordRangeBytes = ByteArrayUtils.byteArrayFromString(coordRangeStr);
				final ArrayOfArrays arrays = new ArrayOfArrays();
				arrays.fromBinary(coordRangeBytes);
				rangeCache = RangeLookupFactory.createMultiRangeLookup(arrays.getCoordinateArrays());
			}
			else {
				throw new IllegalArgumentException(
						"'" + COORDINATE_RANGE_KEY + "' must be set for "
								+ NumericIndexStrategyFilterIterator.class.getName());
			}
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}

	@Override
	public boolean hasTop() {
		return topKey != null;
	}

	@Override
	public void next()
			throws IOException {
		source.next();
		findTop();
	}

	@Override
	public void seek(
			final Range range,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive )
			throws IOException {
		source.seek(
				range,
				columnFamilies,
				inclusive);
		findTop();

	}

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public Value getTopValue() {
		return topValue;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final NumericIndexStrategyFilterIterator iterator = new NumericIndexStrategyFilterIterator();
		iterator.indexStrategy = indexStrategy;
		iterator.rangeCache = rangeCache;
		iterator.source = source.deepCopy(env);
		return iterator;
	}

	private void findTop() {
		topKey = null;
		topValue = null;
		while (source.hasTop()) {
			if (inBounds(source.getTopKey())) {
				topKey = source.getTopKey();
				topValue = source.getTopValue();
				return;
			}
			else {
				try {
					source.next();
				}
				catch (final IOException e) {
					throw new RuntimeException(
							e);
				}
			}
		}
	}

	private boolean inBounds(
			final Key k ) {
		k.getRow(row);
		final GeoWaveKeyImpl key = new GeoWaveKeyImpl(
				row.getBytes(),
				partitionKeyLength);
		final MultiDimensionalCoordinates coordinates = indexStrategy.getCoordinatesPerDimension(
				new ByteArrayId(
						key.getPartitionKey()),
				new ByteArrayId(
						key.getSortKey()));
		return rangeCache.inBounds(coordinates);
	}
}
