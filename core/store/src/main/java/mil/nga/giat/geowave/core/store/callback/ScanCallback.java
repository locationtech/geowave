package mil.nga.giat.geowave.core.store.callback;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 * This interface provides a callback mechanism when scanning entries
 *
 * @param <T>
 *            A generic type for ingested entries
 */
public interface ScanCallback<T, R extends GeoWaveRow>
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
	 * @param entry
	 *            the entry that was ingested
	 * @param row
	 *            the raw row scanned from the table for this entry
	 */
	public void entryScanned(
			final T entry,
			final R row );
}
