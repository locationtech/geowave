package mil.nga.giat.geowave.core.ingest.avro;

import java.io.File;

/**
 * All plugins based off of staged intermediate data (either reading or writing)
 * must implement this interface. For handling intermediate data, the GeoWave
 * ingestion framework has standardized on Avro for java object serialization
 * and an Avro schema must be provided for handling any intermediate data.
 */
public interface AvroPluginBase<T> extends
		AvroSchemaProvider
{
	/**
	 * Converts the supported file into an Avro encoded Java object.
	 * 
	 * @param file
	 *            The file to convert to Avro
	 * @return The Avro encoded Java object
	 */
	public T[] toAvroObjects(
			File file );

}
