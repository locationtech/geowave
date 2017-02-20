package mil.nga.giat.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public class ScanCallbackList<T, R extends GeoWaveRow> implements
		ScanCallback<T, R>,
		Closeable
{
	private final List<ScanCallback<T, R>> callbacks;

	public ScanCallbackList(
			final List<ScanCallback<T, R>> callbacks ) {
		this.callbacks = callbacks;
	}

	@Override
	public void entryScanned(
			final T entry,
			final R rows ) {
		for (final ScanCallback<T, R> callback : callbacks) {
			callback.entryScanned(
					entry,
					rows);
		}
	}

	@Override
	public void close()
			throws IOException {
		for (final ScanCallback<T, R> callback : callbacks) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}

}
