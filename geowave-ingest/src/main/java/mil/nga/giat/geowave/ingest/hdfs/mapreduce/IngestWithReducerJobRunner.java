package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.ingest.AccumuloCommandLineOptions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

public class IngestWithReducerJobRunner extends
		AbstractMapReduceIngest<IngestWithReducer>
{
	public IngestWithReducerJobRunner(
			final AccumuloCommandLineOptions accumuloOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin parentPlugin,
			final IngestWithReducer ingestPlugin ) {
		super(
				accumuloOptions,
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
		job.setMapperClass(IntermediateMapper.class);

		// set mapper output info
		job.setMapOutputKeyClass(Writable.class);
		job.setMapOutputValueClass(Writable.class);
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
