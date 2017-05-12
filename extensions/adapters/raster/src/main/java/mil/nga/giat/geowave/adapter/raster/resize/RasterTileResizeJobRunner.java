package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.coverage.grid.GridCoverage;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.operations.ResizeCommand;
import mil.nga.giat.geowave.adapter.raster.operations.options.RasterTileResizeCommandLineOptions;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class RasterTileResizeJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RasterTileResizeJobRunner.class);

	public static final String NEW_ADAPTER_ID_KEY = "NEW_ADAPTER_ID";
	public static final String OLD_ADAPTER_ID_KEY = "OLD_ADAPTER_ID";

	private final DataStorePluginOptions inputStoreOptions;
	private final DataStorePluginOptions outputStoreOptions;
	protected RasterTileResizeCommandLineOptions rasterResizeOptions;

	public RasterTileResizeJobRunner(
			final DataStorePluginOptions inputStoreOptions,
			final DataStorePluginOptions outputStoreOptions,
			final RasterTileResizeCommandLineOptions rasterResizeOptions ) {
		this.inputStoreOptions = inputStoreOptions;
		this.outputStoreOptions = outputStoreOptions;
		this.rasterResizeOptions = rasterResizeOptions;
	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws IOException,
			InterruptedException,
			ClassNotFoundException {
		Configuration conf = super.getConf();
		if (conf == null) {
			conf = new Configuration();
			setConf(conf);
		}
		GeoWaveConfiguratorBase.setRemoteInvocationParams(
				rasterResizeOptions.getHdfsHostPort(),
				rasterResizeOptions.getJobTrackerOrResourceManHostPort(),
				conf);
		conf.set(
				OLD_ADAPTER_ID_KEY,
				rasterResizeOptions.getInputCoverageName());
		conf.set(
				NEW_ADAPTER_ID_KEY,
				rasterResizeOptions.getOutputCoverageName());
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());

		job.setJobName("Converting " + rasterResizeOptions.getInputCoverageName() + " to tile size="
				+ rasterResizeOptions.getOutputTileSize());

		job.setMapperClass(RasterTileResizeMapper.class);
		job.setCombinerClass(RasterTileResizeCombiner.class);
		job.setReducerClass(RasterTileResizeReducer.class);
		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(GridCoverage.class);
		job.setNumReduceTasks(8);

		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				rasterResizeOptions.getMinSplits());
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				rasterResizeOptions.getMaxSplits());

		GeoWaveInputFormat.setStoreOptions(
				job.getConfiguration(),
				inputStoreOptions);

		final DataAdapter adapter = inputStoreOptions.createAdapterStore().getAdapter(
				new ByteArrayId(
						rasterResizeOptions.getInputCoverageName()));
		if (adapter == null) {
			throw new IllegalArgumentException(
					"Adapter for coverage '" + rasterResizeOptions.getInputCoverageName()
							+ "' does not exist in namespace '" + inputStoreOptions.getGeowaveNamespace() + "'");
		}

		final RasterDataAdapter newAdapter = new RasterDataAdapter(
				(RasterDataAdapter) adapter,
				rasterResizeOptions.getOutputCoverageName(),
				rasterResizeOptions.getOutputTileSize());
		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				adapter);
		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				newAdapter);
		PrimaryIndex index = null;
		final IndexStore indexStore = inputStoreOptions.createIndexStore();
		if (rasterResizeOptions.getIndexId() != null) {
			index = (PrimaryIndex) indexStore.getIndex(new ByteArrayId(
					rasterResizeOptions.getIndexId()));
		}
		if (index == null) {
			try (CloseableIterator<Index<?, ?>> indices = indexStore.getIndices()) {
				index = (PrimaryIndex) indices.next();
			}
			if (index == null) {
				throw new IllegalArgumentException(
						"Index does not exist in namespace '" + inputStoreOptions.getGeowaveNamespace() + "'");
			}
		}
		GeoWaveOutputFormat.setStoreOptions(
				job.getConfiguration(),
				outputStoreOptions);
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				index);
		final DataStore store = outputStoreOptions.createDataStore();
		final IndexWriter writer = store.createWriter(
				newAdapter,
				index);
		boolean retVal = false;
		try {
			retVal = job.waitForCompletion(true);
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Error waiting for map reduce tile resize job: ",
					ex);
		}
		finally {
			writer.close();
		}
		return retVal ? 0 : 1;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final ConfigOptions opts = new ConfigOptions();
		final OperationParser parser = new OperationParser();
		parser.addAdditionalObject(opts);
		final ResizeCommand command = new ResizeCommand();
		final CommandLineOperationParams params = parser.parse(
				command,
				args);
		opts.prepare(params);
		final int res = ToolRunner.run(
				new Configuration(),
				command.createRunner(params),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {

		// parse args to find command line etc...

		return runJob();
	}

}