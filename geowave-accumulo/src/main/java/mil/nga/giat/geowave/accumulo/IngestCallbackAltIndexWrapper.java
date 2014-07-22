package mil.nga.giat.geowave.accumulo;

import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.IngestCallback;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

public class IngestCallbackAltIndexWrapper<T> implements
		IngestCallback<T>
{
	final private IngestCallback<T> callback;
	final private Writer writer;
	final WritableDataAdapter<T> writeableAdapter;

	public IngestCallbackAltIndexWrapper(
			final IngestCallback<T> callback,
			final Writer writer,
			final WritableDataAdapter<T> writeableAdapter ) {
		super();
		this.callback = callback;
		this.writer = writer;
		this.writeableAdapter = writeableAdapter;
	}

	@Override
	public void entryIngested(
			final List<ByteArrayId> rowIds,
			final T entry ) {
		AccumuloUtils.writeAltIndex(
				writeableAdapter,
				rowIds,
				entry,
				writer);
		if (callback != null) {
			callback.entryIngested(
					rowIds,
					entry);
		}
	}
}
