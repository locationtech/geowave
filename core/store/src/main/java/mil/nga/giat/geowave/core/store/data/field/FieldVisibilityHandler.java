package mil.nga.giat.geowave.core.store.data.field;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * This class must be implemented to perform per field value visibility
 * decisions. The byte array that is returned will be used directly in the
 * visibility column for Accumulo.
 * 
 * @param <RowType>
 * @param <FieldType>
 */
public interface FieldVisibilityHandler<RowType, FieldType>
{
	/**
	 * Determine visibility on a per field basis.
	 * 
	 * @param rowValue
	 *            The value for the full row.
	 * @param fieldId
	 *            The ID of the field for which to determine visibility
	 * @param fieldValue
	 *            The value of the field to determine visibility
	 * @return The visibility for a field
	 */
	public byte[] getVisibility(
			RowType rowValue,
			ByteArrayId fieldId,
			FieldType fieldValue );

}
