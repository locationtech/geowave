package mil.nga.giat.geowave.store;

/**
 * This interface provides a callback mechanism when ingesting a collection of
 * entries to receive the row IDs where each entry is ingested
 * 
 * @param <T>
 *            A generic type for ingested entries
 */
public interface IngestCallback<T>
{
	/**
	 * This will be called after an entry is successfully ingested with the row
	 * IDs that were used
	 * 
	 * @param entryInfo
	 *            information regarding what was written to include the
	 *            insertion row IDs, fields, and visibilities
	 * @param entry
	 *            the entry that was ingested
	 */
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			T entry );
}
