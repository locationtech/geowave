package mil.nga.giat.geowave.core.store.data;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;

/**
 * This interface is used to read data from a row in a GeoWave data store.
 * 
 * @param <FieldType>
 *            The binding class of this field
 */
public interface DataReader<FieldType>
{
	/**
	 * Get a reader for an individual field.
	 * 
	 * @param fieldId
	 *            the ID of the field
	 * @return the FieldReader for the given ID
	 */
	public FieldReader<FieldType> getReader(
			ByteArrayId fieldId );

}
