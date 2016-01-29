package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.data.DataReader;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

/**
 * This interface describes the common fields for all of the data within the
 * index. It is up to data adapters to map (encode) the native fields to these
 * common fields for persistence.
 */
public interface CommonIndexModel extends
		DataReader<CommonIndexValue>,
		DataWriter<Object, CommonIndexValue>,
		Persistable
{
	public NumericDimensionField<? extends CommonIndexValue>[] getDimensions();

	public String getId();
}
