package mil.nga.giat.geowave.ingest.local;

import java.io.File;

import mil.nga.giat.geowave.ingest.IngestPluginBase;
import mil.nga.giat.geowave.store.index.Index;

public interface LocalFileIngestPlugin<O> extends
		LocalPluginBase,
		IngestPluginBase<File, O>
{
	public Index[] getSupportedIndices();
}
