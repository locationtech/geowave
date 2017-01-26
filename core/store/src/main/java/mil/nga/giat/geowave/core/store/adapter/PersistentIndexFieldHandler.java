package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is a persistable version of the IndexFieldHandler so that customized
 * field handlers can be automatically persisted with the data adapter. By
 * default the field handlers assume that they can be recreated without custom
 * serialization necessary but if it is necessary, the field handler should
 * implement this interface.
 * 
 * @param <RowType>
 * @param <IndexFieldType>
 * @param <NativeFieldType>
 */
public interface PersistentIndexFieldHandler<RowType, IndexFieldType extends CommonIndexValue, NativeFieldType> extends
		IndexFieldHandler<RowType, IndexFieldType, NativeFieldType>,
		Persistable
{

}
