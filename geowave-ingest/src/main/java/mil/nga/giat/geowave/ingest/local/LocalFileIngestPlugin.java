package mil.nga.giat.geowave.ingest.local;

import java.io.File;

import mil.nga.giat.geowave.ingest.IngestPluginBase;
import mil.nga.giat.geowave.store.index.Index;

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
		IngestPluginBase<File, O>
{
	/**
	 * Get an array of indices that are supported by this ingestion
	 * implementation. This should be the full set of possible indices to use
	 * for this ingest type (for example both spatial and spatial-temporal, or
	 * perhaps just one).
	 * 
	 * @return the array of indices that are supported by this ingestion
	 *         implementation
	 */
	public Index[] getSupportedIndices();
}
