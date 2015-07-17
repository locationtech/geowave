package mil.nga.giat.geowave.core.ingest.avro;

import mil.nga.giat.geowave.core.ingest.IndexProvider;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.local.LocalPluginBase;

/**
 * This is the main plugin interface for reading from a local file system, and
 * formatting intermediate data (for example, to HDFS or to Kafka for further
 * processing or ingest) from any file that is supported to Avro.
 * 
 * @param <I>
 *            The type for the input data
 * @param <O>
 *            The type that represents each data entry being ingested
 */
public interface AvroFormatPlugin<I, O> extends
		AvroPluginBase<I>,
		LocalPluginBase,
		IndexProvider
{

	/**
	 * An implementation of ingestion that ingests Avro Java objects into
	 * GeoWave
	 * 
	 * @return The implementation for ingestion from Avro
	 */
	public IngestPluginBase<I, O> getIngestWithAvroPlugin();

}
