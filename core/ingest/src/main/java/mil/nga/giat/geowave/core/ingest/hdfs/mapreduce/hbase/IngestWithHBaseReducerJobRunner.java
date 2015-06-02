package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IntermediateKeyValueMapper;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author viggy Functionality similar to
 *         <code> IngestWithReducerJobRunner </code>
 */
public class IngestWithHBaseReducerJobRunner extends
		AbstractMapReduceHBaseIngest<IngestWithReducer>
{
	public IngestWithHBaseReducerJobRunner(
			final DataStoreCommandLineOptions options,
			final IngestCommandLineOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin parentPlugin,
			final IngestWithReducer ingestPlugin ) {
		super(
				options,
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
				IngestWithHBaseReducer.class);
		// set mapper output info
		job.setMapOutputKeyClass(genericClasses[1]);
		job.setMapOutputValueClass(genericClasses[2]);
	}

	@Override
	protected void setupReducer(
			final Job job ) {
		job.setReducerClass(IngestHBaseReducer.class);
		if (job.getNumReduceTasks() <= 1) {
			// the default is one reducer, if its only one, set it to 8 as the
			// default
			job.setNumReduceTasks(8);
		}
	}

}
