package mil.nga.giat.geowave.core.ingest.avro;

import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.local.LocalPluginBase;
import mil.nga.giat.geowave.core.store.index.Index;

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
		LocalPluginBase
{

	/**
	 * An implementation of ingestion that ingests Avro Java objects into
	 * GeoWave
	 * 
	 * @return The implementation for ingestion from Avro
	 */
	public IngestPluginBase<I, O> getIngestWithAvroPlugin();

	/**
	 * Get an array of indices that are supported by this ingestion
	 * implementation. This should be the full set of possible indices to use
	 * for this ingest type (for example both spatial and spatial-temporal, or
	 * perhaps just one).
	 * 
	 * @return the array of indices that are supported by this ingestion
	 *         implementation
	 */
	public Index[] getSupportedIndices();

}
