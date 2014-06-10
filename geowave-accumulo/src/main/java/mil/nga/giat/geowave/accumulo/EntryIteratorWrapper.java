package mil.nga.giat.geowave.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This is used internally to translate Accumulo rows into native objects (using
 * the appropriate data adapter). It also performs any client-side filtering. It
 * will peek at the next entry in the accumulo iterator to always maintain a
 * reference to the next value.
 * 
 * @param <T>
 *            The type for the entry
 */
public class EntryIteratorWrapper<T> implements
		Iterator<T>
{
	private final AdapterStore adapterStore;
	private final Index index;
	private final Iterator<Entry<Key, Value>> scannerIt;
	private final QueryFilter clientFilter;

	private T nextValue;

	public EntryIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Iterator<Entry<Key, Value>> scannerIt,
			final QueryFilter clientFilter ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		findNext();
	}

	private void findNext() {
		while (scannerIt.hasNext()) {
			final Entry<Key, Value> row = scannerIt.next();
			final T decodedValue = decodeRow(
					row,
					clientFilter,
					index);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
		nextValue = null;
	}

	private T decodeRow(
			final Entry<Key, Value> row,
			final QueryFilter clientFilter,
			final Index index ) {
		return AccumuloUtils.decodeRow(
				row.getKey(),
				row.getValue(),
				null,
				adapterStore,
				clientFilter,
				index);
	}

	@Override
	public boolean hasNext() {
		return nextValue != null;
	}

	@Override
	public T next() {
		final T previousNext = nextValue;
		findNext();
		return previousNext;
	}

	@Override
	public void remove() {
		// TODO what should we do here considering the scanning iterator is
		// already past the current entry? it probably doesn't matter much as
		// this is not called in practice

		// scannerIt.remove();
	}

}
