package mil.nga.giat.geowave.datastore.accumulo.util;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

public class AltIndexIngestCallback<T> implements
		IngestCallback<T>
{
	final private Writer writer;
	final WritableDataAdapter<T> writeableAdapter;

	public AltIndexIngestCallback(
			final Writer writer,
			final WritableDataAdapter<T> writeableAdapter ) {
		this.writer = writer;
		this.writeableAdapter = writeableAdapter;
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		AccumuloUtils.writeAltIndex(
				writeableAdapter,
				entryInfo,
				entry,
				writer);
	}
}
