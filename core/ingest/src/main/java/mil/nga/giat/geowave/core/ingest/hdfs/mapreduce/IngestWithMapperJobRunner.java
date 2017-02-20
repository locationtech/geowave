package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * This will run the mapper only ingest process.
 */
public class IngestWithMapperJobRunner extends
		AbstractMapReduceIngest<IngestWithMapper<?, ?>>
{

	public IngestWithMapperJobRunner(
			final DataStorePluginOptions storeOptions,
			final List<IndexPluginOptions> indexOptions,
			final VisibilityOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin<?, ?> plugin,
			final IngestWithMapper<?, ?> mapperIngest ) {
		super(
				storeOptions,
				indexOptions,
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
