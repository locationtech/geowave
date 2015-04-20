package mil.nga.giat.geowave.core.store;

/**
 * This interface provides a callback mechanism when scanning entries
 * 
 * @param <T>
 *            A generic type for ingested entries
 */
public interface ScanCallback<T>
{
	/**
	 * This will be called after an entry is successfully scanned with the row
	 * IDs that were used. Deduplication, if performed, occurs prior to calling
	 * this method.
	 * 
	 * Without or without de-duplication, row ids are not consolidate, thus each
	 * entry only contains one row id. If the entry is not de-dupped, then the
	 * entry this method is called for each duplicate, each with a different row
	 * id.
	 * 
	 * @param entryInfo
	 *            information regarding what was scanned to include the
	 *            insertion row IDs, fields, and visibilities
	 * @param entry
	 *            the entry that was ingested
	 */
	public void entryScanned(
			final DataStoreEntryInfo entryInfo,
			final T entry );
}
