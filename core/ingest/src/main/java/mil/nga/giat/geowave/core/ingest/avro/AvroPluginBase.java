package mil.nga.giat.geowave.core.ingest.avro;

import org.apache.avro.Schema;

/**
 * All plugins based off of staged intermediate data (either reading or writing)
 * must implement this interface. For handling intermediate data, the GeoWave
 * ingestion framework has standardized on Avro for java object serialization
 * and an Avro schema must be provided for handling any intermediate data.
 */
public interface AvroPluginBase
{
	public Schema getAvroSchemaForHdfsType();

}
