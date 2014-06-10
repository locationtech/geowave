package mil.nga.giat.geowave.store;

import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;

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
	 * @param rowIds
	 *            the insertion row IDs
	 * @param entry
	 *            the entry that was ingested
	 */
	public void entryIngested(
			List<ByteArrayId> rowIds,
			T entry );
}
