package mil.nga.giat.geowave.store;

import java.util.List;

public class IngestCallbackList<T> implements
		IngestCallback<T>
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

}
