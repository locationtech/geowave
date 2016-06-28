package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This is used internally to translate DataStore rows into native objects
 * (using the appropriate data adapter). It also performs any client-side
 * filtering. It will peek at the next entry in the wrapped iterator to always
 * maintain a reference to the next value.
 *
 * @param <T>
 *            The type for the entry
 */
public abstract class EntryIteratorWrapper<T> implements
		Iterator<T>
{
	protected final AdapterStore adapterStore;
	private final PrimaryIndex index;
	private final Iterator scannerIt;
	private final QueryFilter clientFilter;
	protected final ScanCallback<T> scanCallback;
	private final boolean wholeRowEncoding;

	private T nextValue;

	public EntryIteratorWrapper(
			final boolean wholeRowEncoding,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.scanCallback = scanCallback;
		this.wholeRowEncoding = wholeRowEncoding;
	}

	private void findNext() {
		while ((nextValue == null) && scannerIt.hasNext()) {
			final Object row = scannerIt.next();
			final T decodedValue = decodeRow(
					row,
					clientFilter,
					index,
					wholeRowEncoding);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
	}

	protected abstract T decodeRow(
			final Object row,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			boolean wholeRowEncoding );

	@Override
	public boolean hasNext() {
		findNext();
		return nextValue != null;
	}

	@Override
	public T next()
			throws NoSuchElementException {
		if (nextValue == null) {
			findNext();
		}
		final T previousNext = nextValue;
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

}
