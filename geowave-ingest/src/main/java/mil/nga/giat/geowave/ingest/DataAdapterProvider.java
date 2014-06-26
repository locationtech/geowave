package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

public interface DataAdapterProvider<O>
{
	public WritableDataAdapter<O>[] getDataAdapters(
			String globalVisibility );
}
