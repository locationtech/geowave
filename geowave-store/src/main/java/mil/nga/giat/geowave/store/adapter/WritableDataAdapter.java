package mil.nga.giat.geowave.store.adapter;

import mil.nga.giat.geowave.store.data.DataWriter;

/**
 * This extends the basic DataAdapter interface to be able to ingest data in
 * addition to query for it. Any data adapter used for ingest should implement
 * this interface.
 * 
 * @param <T>
 *            The type of entries that this adapter works on.
 */
public interface WritableDataAdapter<T> extends
		DataAdapter<T>,
		DataWriter<T, Object>
{

}
