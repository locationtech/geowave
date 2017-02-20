package mil.nga.giat.geowave.core.store.callback;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

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
	 * @param entry
	 *            the entry that was ingested
	 * @param entryInfo
	 *            information regarding what was written to include the
	 *            insertion row IDs, fields, and visibilities
	 * @param rows
	 *            the rows inserted into the table for this entry
	 */
	public void entryIngested(
			T entry,
			GeoWaveRow... rows );

}
