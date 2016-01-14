package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * This will run the mapper only ingest process.
 */
public class IngestWithMapperJobRunner extends
		AbstractMapReduceIngest<IngestWithMapper<?, ?>>
{

	public IngestWithMapperJobRunner(
			final DataStoreCommandLineOptions dataStoreOptions,
			final IngestCommandLineOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin<?, ?> plugin,
			final IngestWithMapper<?, ?> mapperIngest ) {
		super(
				dataStoreOptions,
				ingestOptions,
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
