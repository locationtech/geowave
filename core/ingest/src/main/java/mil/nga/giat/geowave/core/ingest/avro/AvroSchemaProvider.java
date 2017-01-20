package mil.nga.giat.geowave.core.ingest.avro;

import org.apache.avro.Schema;

public interface AvroSchemaProvider
{

	/**
	 * Returns the Avro schema for the plugin
	 * 
	 * @return the Avro schema for the intermediate data
	 */
	public Schema getAvroSchema();
}
