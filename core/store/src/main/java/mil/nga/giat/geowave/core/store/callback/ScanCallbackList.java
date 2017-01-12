package mil.nga.giat.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public class ScanCallbackList<T, N> implements
		ScanCallback<T, N>,
		Closeable
{
	private final List<ScanCallback<T, N>> callbacks;

	public ScanCallbackList(
			final List<ScanCallback<T, N>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryScanned(
			final DataStoreEntryInfo entryInfo,
			final N nativeDataStoreEntry,
			final T entry ) {
		for (final ScanCallback<T, N> callback : callbacks) {
			callback.entryScanned(
					entryInfo,
					nativeDataStoreEntry,
					entry);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final ScanCallback<T, N> callback : callbacks) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}

}
