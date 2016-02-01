package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class DeleteCallbackList<T> implements
		DeleteCallback<T>,
		Closeable
{
	private final List<DeleteCallback<T>> callbacks;

	public DeleteCallbackList(
			final List<DeleteCallback<T>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final DeleteCallback<T> callback : callbacks) {
			callback.entryDeleted(
					entryInfo,
					entry);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final DeleteCallback<T> callback : callbacks) {
			if (callback instanceof Closeable) ((Closeable) callback).close();
		}
	}

}
