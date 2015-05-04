package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.data.DataReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

/**
 * This interface should be implemented by any custom data type that must be
 * stored in the Accumulo index. It enables storing and retrieving the data, as
 * well as translating the data into values and queries that can be used to
 * index. Additionally, each entry is responsible for providing visibility if
 * applicable.
 * 
 * @param <T>
 *            The type for the data elements that are being adapted
 */
public interface DataAdapter<T> extends
		DataReader<Object>,
		Persistable
{
	/**
	 * Return the adapter ID
	 * 
	 * @return a unique identifier for this adapter
	 */
	public ByteArrayId getAdapterId();

	public boolean isSupported(
			T entry );

	public ByteArrayId getDataId(
			T entry );

	public T decode(
			IndexedAdapterPersistenceEncoding data,
			Index index );

	public AdapterPersistenceEncoding encode(
			T entry,
			CommonIndexModel indexModel );
}
