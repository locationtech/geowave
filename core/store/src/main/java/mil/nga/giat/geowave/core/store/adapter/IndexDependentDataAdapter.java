package mil.nga.giat.geowave.core.store.adapter;

import java.util.Iterator;

import mil.nga.giat.geowave.core.store.index.Index;

public interface IndexDependentDataAdapter<T> extends
		WritableDataAdapter<T>
{
	public Iterator<T> convertToIndex(
			Index index,
			T originalEntry );
}
