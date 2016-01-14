package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class ScanCallbackList<T> implements
		ScanCallback<T>,
		Closeable
{
	private final List<ScanCallback<T>> callbacks;

	public ScanCallbackList(
			final List<ScanCallback<T>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryScanned(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final ScanCallback<T> callback : callbacks) {
			callback.entryScanned(
					entryInfo,
					entry);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final ScanCallback<T> callback : callbacks) {
			if (callback instanceof Closeable) ((Closeable) callback).close();
		}
	}

}
