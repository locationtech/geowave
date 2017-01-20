package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is internally useful for the AbstractDataAdapter to match field handlers
 * with dimensions by field ID. Any fields with the ID returned by
 * getSupportedIndexFieldIds() will use this handler.
 * 
 * @param <RowType>
 * @param <IndexFieldType>
 * @param <NativeFieldType>
 */
public interface DimensionMatchingIndexFieldHandler<RowType, IndexFieldType extends CommonIndexValue, NativeFieldType> extends
		IndexFieldHandler<RowType, IndexFieldType, NativeFieldType>
{
	/**
	 * Returns the set of field IDs that are supported by this field handler
	 * 
	 * @return the set of field IDs supported
	 */
	public ByteArrayId[] getSupportedIndexFieldIds();
}
