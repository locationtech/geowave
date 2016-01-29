package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class AggregationIterator extends
		QueryFilterIterator
{
	public static final String AGGREGATION_QUERY_ITERATOR_NAME = "GEOWAVE_AGGREGATION_ITERATOR";
	public static final String AGGREGATION_OPTION_NAME = "AGGREGATION";
	public static final String ADAPTER_OPTION_NAME = "ADAPTER";
	public static final int STATISTICS_QUERY_ITERATOR_PRIORITY = 10;
	private Aggregation aggregation;
	private DataAdapter adapter;
	private boolean agregationReturned = false;
	private Text startRowOfAggregation = null;
	private byte[] aggregationBytes;

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
				if (persistenceEncoding instanceof IndexedAdapterPersistenceEncoding) {
					final PersistentDataset<Object> existingExtValues = ((IndexedAdapterPersistenceEncoding) persistenceEncoding).getAdapterExtendedData();
					if (existingExtValues != null) {
						for (final PersistentValue<Object> val : existingExtValues.getValues()) {
							adapterExtendedValues.addValue(val);
						}
					}
				}
				final PersistentDataset<byte[]> stillUnknownValues = new PersistentDataset<byte[]>();
				final List<PersistentValue<byte[]>> unknownDataValues = persistenceEncoding.getUnknownData().getValues();
				for (final PersistentValue<byte[]> v : unknownDataValues) {
					final FieldReader<Object> reader = adapter.getReader(v.getId());
					final Object value = reader.readField(v.getValue());
					adapterExtendedValues.addValue(new PersistentValue<Object>(
							v.getId(),
							value));
				}
				if (persistenceEncoding instanceof IndexedAdapterPersistenceEncoding) {
					for (final PersistentValue<Object> v : ((IndexedAdapterPersistenceEncoding) persistenceEncoding).getAdapterExtendedData().getValues()) {
						adapterExtendedValues.addValue(v);
					}
				}
				final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
						persistenceEncoding.getAdapterId(),
						persistenceEncoding.getDataId(),
						persistenceEncoding.getIndexInsertionId(),
						persistenceEncoding.getDuplicateCount(),
						persistenceEncoding.getCommonData(),
						stillUnknownValues,
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
					aggregation.aggregate(row);
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
			agregationReturned = true;
		}
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		try {
			final String aggregationStr = options.get(AGGREGATION_OPTION_NAME);
			aggregationBytes = ByteArrayUtils.byteArrayFromString(aggregationStr);

			final String adapterStr = options.get(ADAPTER_OPTION_NAME);
			final byte[] adapterBytes = ByteArrayUtils.byteArrayFromString(adapterStr);
			adapter = PersistenceUtils.fromBinary(
					adapterBytes,
					DataAdapter.class);
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
			return new Value(
					PersistenceUtils.toBinary(aggregation));
		}
		return null;
	}

	private boolean hasTopStat() {
		return !agregationReturned && (startRowOfAggregation != null);
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final SortedKeyValueIterator<Key, Value> iterator = super.deepCopy(env);
		if (iterator instanceof AggregationIterator) {
			((AggregationIterator) iterator).startRowOfAggregation = startRowOfAggregation;
			((AggregationIterator) iterator).adapter = adapter;
			((AggregationIterator) iterator).aggregation = aggregation;
			((AggregationIterator) iterator).agregationReturned = agregationReturned;
		}
		return iterator;
	}

	@Override
	public void seek(
			final Range range,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive )
			throws IOException {
		agregationReturned = false;
		aggregation = PersistenceUtils.fromBinary(
				aggregationBytes,
				Aggregation.class);
		startRowOfAggregation = null;
		// TODO consider using a single range for the scanner and decomposing
		// the range within the iterator.
		// for now every seek we have to reset aggregation of the statistic
		super.seek(
				range,
				columnFamilies,
				inclusive);
	}
}