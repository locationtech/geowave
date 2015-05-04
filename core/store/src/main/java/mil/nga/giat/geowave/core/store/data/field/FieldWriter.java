package mil.nga.giat.geowave.core.store.data.field;

/**
 * This interface serializes a field's value into a byte array
 * 
 * @param <RowType>
 * @param <FieldType>
 */
public interface FieldWriter<RowType, FieldType> extends
		FieldVisibilityHandler<RowType, FieldType>
{

	/**
	 * Serializes the entry into binary data that will be stored as the value
	 * for the row
	 * 
	 * @param fieldValue
	 *            The data object to serialize
	 * @return The binary serialization of the data object
	 */
	public byte[] writeField(
			FieldType fieldValue );

}
