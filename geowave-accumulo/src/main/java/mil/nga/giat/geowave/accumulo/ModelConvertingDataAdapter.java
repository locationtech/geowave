package mil.nga.giat.geowave.accumulo;

import mil.nga.giat.geowave.store.index.CommonIndexModel;

public interface ModelConvertingDataAdapter<T> extends
AttachedIteratorDataAdapter<T>
{
	public CommonIndexModel convertModel(
			final CommonIndexModel indexModel );
}
