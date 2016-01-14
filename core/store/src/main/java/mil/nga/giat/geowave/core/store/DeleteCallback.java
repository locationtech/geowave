package mil.nga.giat.geowave.core.store;

/**
 * This interface provides a callback mechanism when deleting a collection of
 * entries.
 * 
 * @param <T>
 *            A generic type for ingested entries
 */
public interface DeleteCallback<T>
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
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry );
}
