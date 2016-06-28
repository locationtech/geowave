package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;

/**
 * This is responsible for persisting adapter/index mappings (either in memory
 * or to disk depending on the implementation).
 */
public interface AdapterIndexMappingStore
{
	public AdapterToIndexMapping getIndicesForAdapter(
			ByteArrayId adapterId );

	/**
	 * If an adapter is already associated with indices and the provided indices
	 * do not match, returns false. Adapter can only be associated with a set of
	 * indices once!
	 * 
	 * @param adapter
	 * @param indices
	 * @throws MismatchedIndexToAdapterMapping
	 *             if the provided associations does match the stored
	 *             associations, if they exist
	 */
	public void addAdapterIndexMapping(
			AdapterToIndexMapping mapping )
			throws MismatchedIndexToAdapterMapping;

	/**
	 * Adapter to index mappings are maintain without regard to visibility
	 * constraints.
	 * 
	 * @param adapterId
	 */
	public void remove(
			ByteArrayId adapterId );

	public void removeAll();
}
