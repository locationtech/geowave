package mil.nga.giat.geowave.accumulo.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

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
	private final AdapterStore adapterStore;
	private final Index index;
	private final Iterator<Entry<Key, Value>> scannerIt;
	private final QueryFilter clientFilter;

	private Entry<GeoWaveInputKey, T> nextValue;

	public InputFormatIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Iterator<Entry<Key, Value>> scannerIt,
			final QueryFilter clientFilter ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
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
		final T value = (T) AccumuloUtils.decodeRow(
				row.getKey(),
				row.getValue(),
				rowId,
				adapterStore,
				clientFilter,
				index);
		if (value == null) {
			return null;
		}
		final GeoWaveInputKey key = new GeoWaveInputKey(
				new ByteArrayId(
						rowId.getAdapterId()),
				new ByteArrayId(
						rowId.getDataId()));
		key.setAccumuloKey(row.getKey());
		return new GeoWaveInputFormatEntry(
				key,
				value);
	}

	@Override
	public boolean hasNext() {
		findNext();
		return nextValue != null;
	}

	@Override
	public Entry<GeoWaveInputKey, T> next() {
		final Entry<GeoWaveInputKey, T> previousNext = nextValue;
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