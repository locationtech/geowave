package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is used by the AbstractDataAdapter to translate between native values
 * and persistence encoded values. The basic implementation of this will perform
 * type matching on the index field type - for explicitly defining the supported
 * dimensions, use DimensionMatchingIndexFieldHandler
 * 
 * @param <RowType>
 * @param <IndexFieldType>
 * @param <NativeFieldType>
 */
public interface IndexFieldHandler<RowType, IndexFieldType extends CommonIndexValue, NativeFieldType>
{
	public ByteArrayId[] getNativeFieldIds();

	public IndexFieldType toIndexValue(
			RowType row );

	public PersistentValue<NativeFieldType>[] toNativeValues(
			IndexFieldType indexValue );
}
