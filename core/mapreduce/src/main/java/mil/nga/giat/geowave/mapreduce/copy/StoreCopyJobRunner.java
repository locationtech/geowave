package mil.nga.giat.geowave.mapreduce.copy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterIndexMappingStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.operations.CopyCommand;
import mil.nga.giat.geowave.mapreduce.operations.CopyCommandOptions;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class StoreCopyJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = Logger.getLogger(StoreCopyJobRunner.class);

	private final DataStorePluginOptions inputStoreOptions;
	private final DataStorePluginOptions outputStoreOptions;
	private final CopyCommandOptions options;
	private final String jobName;

	public StoreCopyJobRunner(
			final DataStorePluginOptions inputStoreOptions,
			final DataStorePluginOptions outputStoreOptions,
			final CopyCommandOptions options,
			final String jobName ) {
		this.inputStoreOptions = inputStoreOptions;
		this.outputStoreOptions = outputStoreOptions;
		this.options = options;
		this.jobName = jobName;
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
				options.getHdfsHostPort(),
				options.getJobTrackerOrResourceManHostPort(),
				conf);

		final Job job = Job.getInstance(conf);

		job.setJarByClass(this.getClass());

		job.setJobName(jobName);

		job.setMapperClass(StoreCopyMapper.class);
		job.setReducerClass(StoreCopyReducer.class);

		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);

		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
		job.setNumReduceTasks(options.getNumReducers());

		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				options.getMinSplits());
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				options.getMaxSplits());

		GeoWaveInputFormat.setStoreOptions(
				job.getConfiguration(),
				inputStoreOptions);

		GeoWaveOutputFormat.setStoreOptions(
				job.getConfiguration(),
				outputStoreOptions);

		final AdapterIndexMappingStore adapterIndexMappingStore = inputStoreOptions.createAdapterIndexMappingStore();
		try (CloseableIterator<DataAdapter<?>> adapterIt = inputStoreOptions.createAdapterStore().getAdapters()) {
			while (adapterIt.hasNext()) {
				DataAdapter<?> dataAdapter = adapterIt.next();

				LOGGER.debug("Adding adapter to output config: "
						+ StringUtils.stringFromBinary(dataAdapter.getAdapterId().getBytes()));

				GeoWaveOutputFormat.addDataAdapter(
						job.getConfiguration(),
						dataAdapter);

				final AdapterToIndexMapping mapping = adapterIndexMappingStore.getIndicesForAdapter(dataAdapter
						.getAdapterId());

				JobContextAdapterIndexMappingStore.addAdapterToIndexMapping(
						job.getConfiguration(),
						mapping);
			}
		}

		try (CloseableIterator<Index<?, ?>> indexIt = inputStoreOptions.createIndexStore().getIndices()) {
			while (indexIt.hasNext()) {
				Index<?, ?> index = indexIt.next();
				if (index instanceof PrimaryIndex) {
					LOGGER.debug("Adding index to output config: "
							+ StringUtils.stringFromBinary(index.getId().getBytes()));

					GeoWaveOutputFormat.addIndex(
							job.getConfiguration(),
							(PrimaryIndex) index);
				}
			}
		}

		boolean retVal = false;
		try {
			retVal = job.waitForCompletion(true);
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Error waiting for store copy job: ",
					ex);
		}

		return retVal ? 0 : 1;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final ConfigOptions opts = new ConfigOptions();
		final OperationParser parser = new OperationParser();
		parser.addAdditionalObject(opts);
		final CopyCommand command = new CopyCommand();
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