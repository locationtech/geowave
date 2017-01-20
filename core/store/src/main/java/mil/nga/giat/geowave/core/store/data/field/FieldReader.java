package mil.nga.giat.geowave.core.store.data.field;

/**
 * This interface deserializes a field from binary data
 * 
 * @param <FieldType>
 */
public interface FieldReader<FieldType>
{

	/**
	 * Deserializes the field from binary data
	 * 
	 * @param fieldData
	 *            The binary serialization of the data object
	 * @return The deserialization of the entry
	 */
	public FieldType readField(
			byte[] fieldData );
}
