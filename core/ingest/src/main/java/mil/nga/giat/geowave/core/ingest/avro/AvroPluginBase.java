package mil.nga.giat.geowave.core.ingest.avro;

import java.io.File;

import org.apache.avro.Schema;

/**
 * All plugins based off of staged intermediate data (either reading or writing)
 * must implement this interface. For handling intermediate data, the GeoWave
 * ingestion framework has standardized on Avro for java object serialization
 * and an Avro schema must be provided for handling any intermediate data.
 */
public interface AvroPluginBase<T>
{
	/**
	 * Returns the Avro schema for the plugin
	 * 
	 * @return the Avro schema for the intermediate data
	 */
	public Schema getAvroSchema();

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
