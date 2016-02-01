package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class IngestCallbackList<T> implements
		IngestCallback<T>,
		Closeable
{
	private final List<IngestCallback<T>> callbacks;

	public IngestCallbackList(
			final List<IngestCallback<T>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final IngestCallback<T> callback : callbacks) {
			callback.entryIngested(
					entryInfo,
					entry);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final IngestCallback<T> callback : callbacks) {
			if (callback instanceof Closeable) ((Closeable) callback).close();
		}
	}

}
