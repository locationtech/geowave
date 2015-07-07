package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.store.index.Index;

public interface IndexProvider
{
	/**
	 * Get an array of indices that are supported by this ingest implementation.
	 * This should be the full set of possible indices to use for this ingest
	 * type (for example both spatial and spatial-temporal, or perhaps just
	 * one).
	 * 
	 * @return the array of indices that are supported by this ingest
	 *         implementation
	 */
	public Index[] getSupportedIndices();

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
	public Index[] getRequiredIndices();
}
