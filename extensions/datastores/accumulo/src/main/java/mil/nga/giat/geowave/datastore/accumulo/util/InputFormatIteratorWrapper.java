package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.HadoopWritableSerializationTool;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.ArrayUtils;

/**
 * This is used internally to translate Accumulo rows into native objects (using
 * the appropriate data adapter). It also performs any client-side filtering. It
 * will peek at the next entry in the accumulo iterator to always maintain a
 * reference to the next value. It maintains the adapter ID, data ID, and
 * original accumulo key in the GeoWaveInputKey for use by the
 * GeoWaveInputFormat.
 * 
 * @param <T>
 *            The type for the entry
 */
public class InputFormatIteratorWrapper<T> implements
		Iterator<Entry<GeoWaveInputKey, T>>
{
	private final Index index;
	private final Iterator<Entry<Key, Value>> scannerIt;
	private final QueryFilter clientFilter;
	private final HadoopWritableSerializationTool serializationTool;
	private final boolean isOutputWritable;
	private Entry<GeoWaveInputKey, T> nextValue;

	public InputFormatIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Iterator<Entry<Key, Value>> scannerIt,
			final boolean isOutputWritable,
			final QueryFilter clientFilter ) {
		this.serializationTool = new HadoopWritableSerializationTool(
				adapterStore);
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.isOutputWritable = isOutputWritable;
	}

	private void findNext() {
		while ((nextValue == null) && scannerIt.hasNext()) {
			final Entry<Key, Value> row = scannerIt.next();
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
			final Entry<Key, Value> row,
			final QueryFilter clientFilter,
			final Index index ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				row.getKey());
		final Object value = AccumuloUtils.decodeRow(
				row.getKey(),
				row.getValue(),
				rowId,
				serializationTool.getAdapterStore(),
				clientFilter,
				index);
		if (value == null) {
			return null;
		}
		final ByteArrayId adapterId = new ByteArrayId(
				rowId.getAdapterId());
		T result = (T) (isOutputWritable ? serializationTool.getHadoopWritableSerializerForAdapter(
				adapterId).toWritable(
				value) : value);
		final GeoWaveInputKey key = new GeoWaveInputKey(
				adapterId,
				new ByteArrayId(
						// if deduplication is disabled, prefix the actual data
						// ID with the index ID concatenated with the insertion
						// ID to gaurantee uniqueness and effectively disable
						// aggregating by only the data ID
						rowId.isDeduplicationEnabled() ? rowId.getDataId() : ArrayUtils.addAll(
								ArrayUtils.addAll(
										index.getId().getBytes(),
										rowId.getInsertionId()),
								rowId.getDataId())));
		key.setAccumuloKey(row.getKey());
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