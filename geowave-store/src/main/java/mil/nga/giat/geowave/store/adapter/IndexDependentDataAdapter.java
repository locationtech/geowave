package mil.nga.giat.geowave.store.adapter;

import java.util.Iterator;

import mil.nga.giat.geowave.store.index.Index;

public interface IndexDependentDataAdapter<T> extends
		WritableDataAdapter<T>
{
	public Iterator<T> convertToIndex(
			Index index,
			T originalEntry );
}
