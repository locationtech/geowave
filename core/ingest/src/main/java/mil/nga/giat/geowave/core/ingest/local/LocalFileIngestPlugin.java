package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;

import mil.nga.giat.geowave.core.ingest.IndexProvider;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;

/**
 * This is the primary plugin for directly ingesting data to GeoWave from local
 * files. It will write any GeoWaveData that is emitted for any supported file.
 * 
 * 
 * @param <O>
 *            The type of data to write to GeoWave
 */
public interface LocalFileIngestPlugin<O> extends
		LocalPluginBase,
		IngestPluginBase<File, O>,
		IndexProvider
{
}
