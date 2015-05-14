package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloCommandLineOptions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * This class will run the ingestion process by using a mapper to aggregate key
 * value pairs and a reducer to ingest data into GeoWave.
 */
public class IngestWithReducerJobRunner extends
		AbstractMapReduceIngest<IngestWithReducer>
{
	public IngestWithReducerJobRunner(
			final AccumuloCommandLineOptions accumuloOptions,
			final IngestCommandLineOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin parentPlugin,
			final IngestWithReducer ingestPlugin ) {
		super(
				accumuloOptions,
				ingestOptions,
				inputFile,
				typeName,
				parentPlugin,
				ingestPlugin);
	}

	@Override
	protected String getIngestDescription() {
		return "with reducer";
	}

	@Override
	protected void setupMapper(
			final Job job ) {
		job.setMapperClass(IntermediateKeyValueMapper.class);
		final Class<?>[] genericClasses = GenericTypeResolver.resolveTypeArguments(
				ingestPlugin.getClass(),
				IngestWithReducer.class);
		// set mapper output info
		job.setMapOutputKeyClass(genericClasses[1]);
		job.setMapOutputValueClass(genericClasses[2]);
	}

	@Override
	protected void setupReducer(
			final Job job ) {
		job.setReducerClass(IngestReducer.class);
		if (job.getNumReduceTasks() <= 1) {
			// the default is one reducer, if its only one, set it to 8 as the
			// default
			job.setNumReduceTasks(8);
		}
	}

}
