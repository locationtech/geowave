package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public interface ModelConvertingDataAdapter<T> extends
		AttachedIteratorDataAdapter<T>
{
	public CommonIndexModel convertModel(
			final CommonIndexModel indexModel );
}
