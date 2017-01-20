package mil.nga.giat.geowave.core.store.adapter;

import java.util.Iterator;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface IndexDependentDataAdapter<T> extends
		WritableDataAdapter<T>
{
	public Iterator<T> convertToIndex(
			PrimaryIndex index,
			T originalEntry );
}
