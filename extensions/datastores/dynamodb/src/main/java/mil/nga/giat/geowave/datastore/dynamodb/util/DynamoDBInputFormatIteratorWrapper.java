package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.commons.lang.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializationTool;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

/**
 * This is used internally to translate Dynamo DB rows into native objects
 * (using the appropriate data adapter). It also performs any client-side
 * filtering. It will peek at the next entry in the HBase iterator to always
 * maintain a reference to the next value. It maintains the adapter ID, data ID,
 * and original DynamoDB key in the GeoWaveInputKey for use by the
 * GeoWaveInputFormat.
 * 
 * @param <T>
 *            The type for the entry
 */
public class DynamoDBInputFormatIteratorWrapper<T> implements
		Iterator<Entry<GeoWaveInputKey, T>>
{
	private final BaseDataStore dataStore;
	private final PrimaryIndex index;
	private final Iterator<DynamoDBRow> scannerIt;
	private final QueryFilter clientFilter;
	private final HadoopWritableSerializationTool serializationTool;
	private final boolean isOutputWritable;
	private Entry<GeoWaveInputKey, T> nextValue;

	public DynamoDBInputFormatIteratorWrapper(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<DynamoDBRow> scannerIt,
			final boolean isOutputWritable,
			final QueryFilter clientFilter ) {
		this.dataStore = dataStore;
		this.serializationTool = new HadoopWritableSerializationTool(
				adapterStore);
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.isOutputWritable = isOutputWritable;
	}

	private void findNext() {
		while ((nextValue == null) && scannerIt.hasNext()) {
			final DynamoDBRow row = scannerIt.next();
			final Entry<GeoWaveInputKey, T> decodedValue = decodeRow(
					row,
					clientFilter,
					index);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Entry<GeoWaveInputKey, T> decodeRow(
			final DynamoDBRow row,
			final QueryFilter clientFilter,
			final PrimaryIndex index ) {
		final Object value = dataStore.decodeRow(
				row,
				true,
				clientFilter,
				null,
				serializationTool.getAdapterStore(),
				index,
				null,
				null,
				true);

		if (value == null) {
			return null;
		}
		final ByteArrayId adapterId = new ByteArrayId(
				row.getAdapterId());
		final T result = (T) (isOutputWritable ? serializationTool.getHadoopWritableSerializerForAdapter(
				adapterId).toWritable(
				value) : value);
		final GeoWaveInputKey key = new GeoWaveInputKey(
				adapterId,
				new ByteArrayId(
						// if deduplication is disabled, prefix the actual data
						// ID with the index ID concatenated with the insertion
						// ID to gaurantee uniqueness and effectively disable
						// aggregating by only the data ID
						row.isDeduplicationEnabled() ? row.getDataId() : ArrayUtils.addAll(
								ArrayUtils.addAll(
										index.getId().getBytes(),
										row.getIndex()),
								row.getDataId())));
		key.setInsertionId(new ByteArrayId(
				row.getIndex()));
		return new GeoWaveInputFormatEntry(
				key,
				result);
	}

	@Override
	public boolean hasNext() {
		findNext();
		return nextValue != null;
	}

	@Override
	public Entry<GeoWaveInputKey, T> next()
			throws NoSuchElementException {
		final Entry<GeoWaveInputKey, T> previousNext = nextValue;
		if (nextValue == null) {
			throw new NoSuchElementException();
		}
		nextValue = null;
		return previousNext;
	}

	@Override
	public void remove() {
		scannerIt.remove();
	}

	private final class GeoWaveInputFormatEntry implements
			Map.Entry<GeoWaveInputKey, T>
	{
		private final GeoWaveInputKey key;
		private T value;

		public GeoWaveInputFormatEntry(
				final GeoWaveInputKey key,
				final T value ) {
			this.key = key;
			this.value = value;
		}

		@Override
		public GeoWaveInputKey getKey() {
			return key;
		}

		@Override
		public T getValue() {
			return value;
		}

		@Override
		public T setValue(
				final T value ) {
			final T old = this.value;
			this.value = value;
			return old;
		}
	}
}