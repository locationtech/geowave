package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.datastore.hbase.HBaseCommandLineOptions;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.output.GeoWaveHBaseOutputKey;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author viggy Functionality similar to
 *         <code> IngestWithMapperJobRunner </code>
 */
public class IngestWithHBaseMapperJobRunner extends
		AbstractMapReduceHBaseIngest<IngestWithMapper>
{

	public IngestWithHBaseMapperJobRunner(
			final HBaseCommandLineOptions options,
			final IngestCommandLineOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin plugin,
			final IngestWithMapper mapperIngest ) {
		super(
				options,
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
		job.setMapperClass(IngestHBaseMapper.class);
		// set mapper output info
		job.setMapOutputKeyClass(GeoWaveHBaseOutputKey.class);
		job.setMapOutputValueClass(Object.class);
	}

}
