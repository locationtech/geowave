package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class StatisticComputingIterator extends
		QueryFilterIterator
{
	private final static Logger LOGGER = Logger.getLogger(StatisticComputingIterator.class);
	public static final String STATISTICS_QUERY_ITERATOR_NAME = "STATISTICS_QUERY_ITERATOR";
	public static final String STATISTICS_OPTION_NAME = "STATS";
	public static final String ADAPTER_OPTION_NAME = "ADAPTER";
	public static final int STATISTICS_QUERY_ITERATOR_PRIORITY = 10;
	private DataStatistics stats;
	private DataAdapter adapter;
	private boolean statReturned = false;
	private Text startRowOfStat = null;
	private byte[] statsBytes;

	public StatisticComputingIterator() {
		super();
	}

	public StatisticComputingIterator(
			final DataStatistics<?> stats ) {
		this.stats = stats;
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
					final List<ByteArrayId> insertionIds = new ArrayList<ByteArrayId>();
					insertionIds.add(encoding.getIndexInsertionId());
					// for now ignore field info
					stats.entryIngested(
							new DataStoreEntryInfo(
									encoding.getDataId().getBytes(),
									insertionIds,
									null),
							row);
					if (startRowOfStat == null) {
						startRowOfStat = currentRow;
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
			statReturned = true;
		}
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		try {
			final String statsStr = options.get(STATISTICS_OPTION_NAME);
			statsBytes = ByteArrayUtils.byteArrayFromString(statsStr);

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
					startRowOfStat);
		}
		return null;
	}

	private Value getTopStatValue() {
		if (hasTopStat()) {
			return new Value(
					PersistenceUtils.toBinary(stats));
		}
		return null;
	}

	private boolean hasTopStat() {
		return !statReturned && (startRowOfStat != null);
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final SortedKeyValueIterator<Key, Value> iterator = super.deepCopy(env);
		if (iterator instanceof StatisticComputingIterator) {
			((StatisticComputingIterator) iterator).startRowOfStat = startRowOfStat;
			((StatisticComputingIterator) iterator).adapter = adapter;
			((StatisticComputingIterator) iterator).stats = stats;
			((StatisticComputingIterator) iterator).statReturned = statReturned;
		}
		return iterator;
	}

	@Override
	public void seek(
			final Range range,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive )
			throws IOException {
		statReturned = false;
		stats = PersistenceUtils.fromBinary(
				statsBytes,
				DataStatistics.class);
		startRowOfStat = null;
		// TODO consider using a single range for the scanner and decomposing
		// the range within the iterator.
		// for now every seek we have to reset aggregation of the statistic
		super.seek(
				range,
				columnFamilies,
				inclusive);
	}
}