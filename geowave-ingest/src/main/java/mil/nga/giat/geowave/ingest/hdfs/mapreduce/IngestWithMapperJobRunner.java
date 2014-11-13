package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.ingest.AccumuloCommandLineOptions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * This will run the mapper only ingest process.
 */
public class IngestWithMapperJobRunner extends
		AbstractMapReduceIngest<IngestWithMapper>
{

	public IngestWithMapperJobRunner(
			final AccumuloCommandLineOptions accumuloOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin plugin,
			final IngestWithMapper mapperIngest ) {
		super(
				accumuloOptions,
				inputFile,
				typeName,
				plugin,
				mapperIngest);
	}

	@Override
	protected void setupReducer(
			final Job job ) {
		job.setNumReduceTasks(0);
	}

	@Override
	protected String getIngestDescription() {
		return "map only";
	}

	@Override
	protected void setupMapper(
			final Job job ) {
		job.setMapperClass(IngestMapper.class);
		// set mapper output info
		job.setMapOutputKeyClass(GeoWaveOutputKey.class);
		job.setMapOutputValueClass(Object.class);
	}

}
