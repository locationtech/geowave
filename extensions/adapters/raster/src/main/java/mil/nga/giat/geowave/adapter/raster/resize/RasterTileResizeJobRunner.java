package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeJobRunner extends
		Configured implements
		Tool,
		CLIOperationDriver
{
	private static final Logger LOGGER = Logger.getLogger(RasterTileResizeJobRunner.class);

	public static final String NEW_ADAPTER_ID_KEY = "NEW_ADAPTER_ID";
	public static final String OLD_ADAPTER_ID_KEY = "OLD_ADAPTER_ID";

	protected DataStoreCommandLineOptions inputDataStoreOptions;
	protected DataStoreCommandLineOptions outputDataStoreOptions;
	protected AdapterStoreCommandLineOptions inputAdapterStoreOptions;
	protected IndexStoreCommandLineOptions inputIndexStoreOptions;
	protected RasterTileResizeCommandLineOptions rasterResizeOptions;

	public RasterTileResizeJobRunner() {

	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws Exception {
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

		job.setJobName("Converting " + rasterResizeOptions.getInputCoverageName() + " to tile size=" + rasterResizeOptions.getOutputTileSize());

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

		GeoWaveInputFormat.setDataStoreName(
				job.getConfiguration(),
				inputDataStoreOptions.getFactory().getName());
		GeoWaveInputFormat.setStoreConfigOptions(
				job.getConfiguration(),
				ConfigUtils.valuesToStrings(
						inputDataStoreOptions.getConfigOptions(),
						inputDataStoreOptions.getFactory().getOptions()));
		GeoWaveInputFormat.setGeoWaveNamespace(
				job.getConfiguration(),
				inputDataStoreOptions.getNamespace());

		final DataAdapter adapter = inputAdapterStoreOptions.createStore().getAdapter(
				new ByteArrayId(
						rasterResizeOptions.getInputCoverageName()));
		if (adapter == null) {
			throw new IllegalArgumentException(
					"Adapter for coverage '" + rasterResizeOptions.getInputCoverageName() + "' does not exist in namespace '" + inputAdapterStoreOptions.getNamespace() + "'");
		}

		final RasterDataAdapter newAdapter = new RasterDataAdapter(
				(RasterDataAdapter) adapter,
				rasterResizeOptions.getOutputCoverageName(),
				rasterResizeOptions.getOutputTileSize(),
				new NoDataMergeStrategy());
		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				adapter);
		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				newAdapter);
		PrimaryIndex index = null;
		final IndexStore indexStore = inputIndexStoreOptions.createStore();
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
						"Index does not exist in namespace '" + inputIndexStoreOptions.getNamespace() + "'");
			}
		}
		GeoWaveOutputFormat.setDataStoreName(
				job.getConfiguration(),
				outputDataStoreOptions.getFactory().getName());
		GeoWaveOutputFormat.setStoreConfigOptions(
				job.getConfiguration(),
				ConfigUtils.valuesToStrings(
						outputDataStoreOptions.getConfigOptions(),
						outputDataStoreOptions.getFactory().getOptions()));
		GeoWaveOutputFormat.setGeoWaveNamespace(
				job.getConfiguration(),
				outputDataStoreOptions.getNamespace());
		JobContextIndexStore.addIndex(
				job.getConfiguration(),
				index);
		final DataStore store = outputDataStoreOptions.createStore();
		final IndexWriter writer = store.createIndexWriter(
				index,
				DataStoreUtils.DEFAULT_VISIBILITY);
		writer.setupAdapter(newAdapter);
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
		final int res = ToolRunner.run(
				new Configuration(),
				new RasterTileResizeJobRunner(),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(
				"input_",
				allOptions);
		DataStoreCommandLineOptions.applyOptions(
				"output_",
				allOptions);
		AdapterStoreCommandLineOptions.applyOptions(
				"input_",
				allOptions);
		IndexStoreCommandLineOptions.applyOptions(
				"input_",
				allOptions);

		RasterTileResizeCommandLineOptions.applyOptions(allOptions);
		Exception exception = null;
		final BasicParser parser = new BasicParser();
		CommandLine commandLine = parser.parse(
				allOptions,
				args,
				true);
		CommandLineResult<DataStoreCommandLineOptions> inputDataStoreOptionsResult = null;
		CommandLineResult<DataStoreCommandLineOptions> outputDataStoreOptionsResult = null;
		CommandLineResult<AdapterStoreCommandLineOptions> inputAdapterStoreOptionsResult = null;
		CommandLineResult<IndexStoreCommandLineOptions> inputIndexStoreOptionsResult = null;
		boolean newCommandLine = false;
		do {
			newCommandLine = false;
			inputDataStoreOptionsResult = null;
			outputDataStoreOptionsResult = null;
			inputAdapterStoreOptionsResult = null;
			inputIndexStoreOptionsResult = null;
			exception = null;
			rasterResizeOptions = RasterTileResizeCommandLineOptions.parseOptions(commandLine);
			try {
				inputDataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
						"input_",
						allOptions,
						commandLine);
			}
			catch (final Exception e) {
				exception = e;
			}
			if ((inputDataStoreOptionsResult != null) && inputDataStoreOptionsResult.isCommandLineChange()) {
				// commandLine = inputDataStoreOptionsResult.getCommandLine();
				for (final Option o : commandLine.getOptions()) {
					final Option optClone = ((Option) o.clone());
					optClone.setRequired(false);
					allOptions.addOption(optClone);
				}
			}

			try {
				inputAdapterStoreOptionsResult = AdapterStoreCommandLineOptions.parseOptions(
						"input_",
						allOptions,
						commandLine);
			}
			catch (final Exception e) {
				exception = e;
			}
			if ((inputAdapterStoreOptionsResult != null) && inputAdapterStoreOptionsResult.isCommandLineChange()) {
				// commandLine =
				// inputAdapterStoreOptionsResult.getCommandLine();
				for (final Option o : commandLine.getOptions()) {
					final Option optClone = ((Option) o.clone());
					optClone.setRequired(false);
					allOptions.addOption(optClone);
				}
				// newCommandLine = true;
				// continue;
			}
			try {
				inputIndexStoreOptionsResult = IndexStoreCommandLineOptions.parseOptions(
						"input_",
						allOptions,
						commandLine);
			}
			catch (final Exception e) {
				exception = e;
			}
			if ((inputIndexStoreOptionsResult != null) && inputIndexStoreOptionsResult.isCommandLineChange()) {
				commandLine = inputIndexStoreOptionsResult.getCommandLine();
				for (final Option o : commandLine.getOptions()) {
					final Option optClone = ((Option) o.clone());
					optClone.setRequired(false);
					allOptions.addOption(optClone);
				}
				// newCommandLine = true;
				// continue;
			}
			try {
				outputDataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
						"output_",
						allOptions,
						commandLine);
			}
			catch (final Exception e) {
				exception = e;
			}
			if ((outputDataStoreOptionsResult != null) && outputDataStoreOptionsResult.isCommandLineChange()) {
				// commandLine = outputDataStoreOptionsResult.getCommandLine();
				for (final Option o : commandLine.getOptions()) {
					final Option optClone = ((Option) o.clone());
					optClone.setRequired(false);
					allOptions.addOption(optClone);
				}
				// newCommandLine = true;
				// continue;
			}
		}
		while (newCommandLine);
		if (exception != null) {
			throw exception;
		}
		inputDataStoreOptions = inputDataStoreOptionsResult.getResult();
		inputAdapterStoreOptions = inputAdapterStoreOptionsResult.getResult();
		inputIndexStoreOptions = inputIndexStoreOptionsResult.getResult();
		outputDataStoreOptions = outputDataStoreOptionsResult.getResult();
		return runJob();
	}

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		try {
			return run(args) == 0;
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to run operation",
					e);
			return false;
		}
	}
}