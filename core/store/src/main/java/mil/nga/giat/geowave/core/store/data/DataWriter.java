package mil.nga.giat.geowave.core.store.data;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

/**
 * This interface is used to write data for a row in a GeoWave data store.
 * 
 * @param <RowType>
 *            The binding class of this row
 * @param <FieldType>
 *            The binding class of this field
 */
public interface DataWriter<RowType, FieldType>
{
	/**
	 * Get a writer for an individual field given the ID.
	 * 
	 * @param fieldId
	 *            the unique field ID
	 * @return the writer for the given field
	 */
	public FieldWriter<RowType, FieldType> getWriter(
			ByteArrayId fieldId );
}
