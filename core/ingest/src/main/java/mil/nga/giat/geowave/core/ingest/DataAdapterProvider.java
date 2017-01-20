package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This interface is applicable for plugins that need to provide writable data
 * adapters for ingest.
 * 
 * @param <O>
 *            the java type for the data being ingested
 */
public interface DataAdapterProvider<T>
{
	/**
	 * Get all writable adapters used by this plugin
	 * 
	 * @param globalVisibility
	 *            If on the command-line the user specifies a global visibility
	 *            to write to the visibility column in GeoWave, it is passed
	 *            along here. It is assumed that this is the same visibility
	 *            string that will be passed to IngestPluginBase.toGeoWaveData()
	 * @return An array of adapters that may be used by this plugin
	 */
	public WritableDataAdapter<T>[] getDataAdapters(
			String globalVisibility );

	/**
	 * return a set of classes that can be indexed by this data adapter
	 * provider, used for compatibility checking with an index provider
	 * 
	 * @return the classes that are indexable by this adapter provider
	 */
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes();
}
