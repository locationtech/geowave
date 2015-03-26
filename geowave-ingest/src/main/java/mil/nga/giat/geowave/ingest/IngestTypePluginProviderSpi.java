package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;

/**
 * This interface can be injected and automatically discovered using SPI to
 * provide a new ingest type to the GeoWave ingestion framework. It is not
 * required that a new ingest type implement all of the plugins. However, each
 * plugin directly corresponds to a user selected operation and only the plugins
 * that are supported will result in usable operations.
 * 
 * @param <I>
 *            The type for intermediate data
 * @param <O>
 *            The type for the resulting data that is ingested into GeoWave
 */
public interface IngestTypePluginProviderSpi<I, O>
{
	/**
	 * This plugin will be used by the ingestion framework to stage intermediate
	 * data to HDFS from a local filesystem.
	 * 
	 * @return The plugin for staging to HDFS if it is supported
	 * @throws UnsupportedOperationException
	 *             If staging data is not supported (generally this implies that
	 *             ingesting using map-reduce will not be supported)
	 */
	public StageToHdfsPlugin<I> getStageToHdfsPlugin()
			throws UnsupportedOperationException;

	/**
	 * This plugin will be used by the ingestion framework to read data from
	 * HDFS in the form of the intermediate data format, and translate the
	 * intermediate data into the data entries that will be written in GeoWave.
	 * 
	 * @return The plugin for ingesting data from HDFS
	 * @throws UnsupportedOperationException
	 *             If ingesting intermediate data from HDFS is not supported
	 */
	public IngestFromHdfsPlugin<I, O> getIngestFromHdfsPlugin()
			throws UnsupportedOperationException;

	/**
	 * This plugin will be used by the ingestion framework to read data from a
	 * local file system, and translate supported files into the data entries
	 * that will be written directly in GeoWave.
	 * 
	 * @return The plugin for ingesting data from a local file system directly
	 *         into GeoWave
	 * @throws UnsupportedOperationException
	 *             If ingesting data directly from a local file system is not
	 *             supported
	 */
	public LocalFileIngestPlugin<O> getLocalFileIngestPlugin()
			throws UnsupportedOperationException;

	/**
	 * This will represent the name for the type that is registered with the
	 * ingest framework and presented as a data type option via the commandline.
	 * For consistency, this name is preferably lower-case and without spaces,
	 * and should uniquely identify the data type as much as possible.
	 * 
	 * @return The name that will be associated with this type
	 */
	public String getIngestTypeName();

	/**
	 * This is a means for a plugin to provide custom command-line options. If
	 * this is null, there will be no custom options added.
	 * 
	 * 
	 * @return The ingest type's option provider or null for no custom options
	 */
	public IngestTypeOptionProvider getIngestTypeOptionProvider();

	/**
	 * This is a user-friendly full description of the data type that this
	 * plugin provider supports. It will be presented to the command-line user
	 * as help when the registered data types are listed.
	 * 
	 * @return The user-friendly full description for this data type
	 */
	public String getIngestTypeDescription();
}
