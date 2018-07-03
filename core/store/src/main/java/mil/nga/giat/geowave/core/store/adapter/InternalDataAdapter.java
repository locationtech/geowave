package mil.nga.giat.geowave.core.store.adapter;

public interface InternalDataAdapter<T> extends
		WritableDataAdapter<T>
{
	public short getInternalAdapterId();

	public WritableDataAdapter<?> getAdapter();
}
