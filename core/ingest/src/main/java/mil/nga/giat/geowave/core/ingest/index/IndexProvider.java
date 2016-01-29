package mil.nga.giat.geowave.core.ingest.index;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface IndexProvider
{

	/**
	 * Get an array of indices that are required by this ingest implementation.
	 * This should be a subset of supported indices. All of these indices will
	 * automatically be persisted with GeoWave's metadata store (and in the job
	 * configuration if run as a job), whereas indices that are just "supported"
	 * will not automatically be persisted (only if they are the primary index).
	 * This is primarily useful if there is a supplemental index required by the
	 * ingest process that is not the primary index.
	 * 
	 * @return the array of indices that are supported by this ingest
	 *         implementation
	 */
	public PrimaryIndex[] getRequiredIndices();
}
