package mil.nga.giat.geowave.core.store.callback;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 * This interface provides a callback mechanism when deleting a collection of
 * entries.
 *
 * @param <T>
 *            A generic type for entries
 * @param <R>
 *            A generic type for rows
 */
public interface DeleteCallback<T, R extends GeoWaveRow>
{
	/**
	 * This will be called after an entry is successfully deleted with the row
	 * IDs that were used
	 *
	 * @param row
	 *            the raw row that was deleted
	 * @param entry
	 *            the entry that was deleted
	 */
	public void entryDeleted(
			final T entry,
			final R... rows );
}
