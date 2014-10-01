package mil.nga.giat.geowave.accumulo;

import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

public interface AttachedIteratorDataAdapter<T> extends
		WritableDataAdapter<T>
{
	public static final String ATTACHED_ITERATOR_CACHE_ID = "AttachedIterators";

	public IteratorConfig[] getAttachedIteratorConfig();
}
