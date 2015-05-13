package mil.nga.giat.geowave.core.ingest.avro;

import java.io.File;

import mil.nga.giat.geowave.core.ingest.local.LocalPluginBase;

/**
 * This is the main plugin interface for reading from a local file system, and
 * staging intermediate data (for example, to HDFS or to Kafka for further
 * processing or ingest) from any file that is supported.
 * 
 * @param <T>
 *            the type for intermediate data, it must match the type supported
 *            by the Avro schema
 */
public interface StageToAvroPlugin<T> extends
		AvroPluginBase,
		LocalPluginBase
{

	/**
	 * Read a file from the local file system and emit an array of intermediate
	 * data elements that will be serialized.
	 * 
	 * @param f
	 *            a local file that is supported by this plugin
	 * @return an array of intermediate data objects that will be serialized
	 */
	public T[] toAvroObjects(
			File f );
}
