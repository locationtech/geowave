package mil.nga.giat.geowave.accumulo.util;

import java.util.List;

import mil.nga.giat.geowave.accumulo.Writer;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.IngestCallback;
import mil.nga.giat.geowave.store.IngestEntryInfo;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

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
			final IngestEntryInfo entryInfo,
			final T entry ) {
		AccumuloUtils.writeAltIndex(
				writeableAdapter,
				entryInfo,
				entry,
				writer);
	}
}
