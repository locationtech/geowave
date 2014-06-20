package mil.nga.giat.geowave.ingest.local;

import java.io.File;

import mil.nga.giat.geowave.ingest.IngestPluginBase;

public interface LocalFileIngestPlugin<O> extends
		LocalPluginBase,
		IngestPluginBase<File, O>
{

}
