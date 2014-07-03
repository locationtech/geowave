package mil.nga.giat.geowave.ingest.hdfs;

import java.io.File;

import mil.nga.giat.geowave.ingest.local.LocalPluginBase;

/**
 * This is the main plugin interface for reading from a local file system, and
 * staging intermediate data to HDFS from any file that is supported.
 * 
 * @param <T>
 *            the type for intermediate data, it must match the type supported
 *            by the Avro schema
 */
public interface StageToHdfsPlugin<T> extends
		HdfsPluginBase,
		LocalPluginBase
{

	/**
	 * Read a file from the local file system and emit an array of intermediate
	 * data elements that will be serialized and staged to HDFS.
	 * 
	 * @param f
	 *            a local file that is supported by this plugin
	 * @return an array of intermediate data objects that will be serialized and
	 *         written to HDFS
	 */
	public T[] toHdfsObjects(
			File f );
}
