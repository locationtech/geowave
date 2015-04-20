package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

/**
 * This is used by the AbstractDataAdapter to get individual field values from
 * the row
 * 
 * @param <RowType>
 * @param <FieldType>
 */
public interface NativeFieldHandler<RowType, FieldType>
{
	/**
	 * Get the field ID that this handler supports
	 * 
	 * @return the field ID supported
	 */
	public ByteArrayId getFieldId();

	/**
	 * Get the field value from the row
	 * 
	 * @param row
	 *            the row
	 * @return the field value
	 */
	public FieldType getFieldValue(
			RowType row );

	/**
	 * This is used internally by the AbstractDataAdapter to build a row from a
	 * set of field values
	 * 
	 * @param <RowType>
	 * @param <FieldType>
	 */
	public static interface RowBuilder<RowType, FieldType>
	{
		/**
		 * Set a field ID/value pair
		 * 
		 * @param fieldValue
		 *            the field ID/value pair
		 */
		public void setField(
				PersistentValue<FieldType> fieldValue );

		/**
		 * Create a row with the previously set fields
		 * 
		 * @param dataId
		 *            the unique data ID for the row
		 * @return the row
		 */
		public RowType buildRow(
				ByteArrayId dataId );
	}
}
