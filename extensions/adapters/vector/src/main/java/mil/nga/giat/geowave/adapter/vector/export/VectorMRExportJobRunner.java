package mil.nga.giat.geowave.adapter.vector.export;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.text.cql2.CQLException;

import com.beust.jcommander.JCommander;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

public class VectorMRExportJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = LoggerFactory.getLogger(VectorMRExportCommand.class);

	public static final String BATCH_SIZE_KEY = "BATCH_SIZE";
	private final DataStorePluginOptions storeOptions;
	private final VectorMRExportOptions mrOptions;
	private final String hdfsHostPort;
	private final String hdfsPath;

	public VectorMRExportJobRunner(
			final DataStorePluginOptions storeOptions,
			final VectorMRExportOptions mrOptions,
			final String hdfsHostPort,
			final String hdfsPath ) {
		this.storeOptions = storeOptions;
		this.mrOptions = mrOptions;
		this.hdfsHostPort = hdfsHostPort;
		this.hdfsPath = hdfsPath;
	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws CQLException,
			IOException,
			InterruptedException,
			ClassNotFoundException {
		Configuration conf = super.getConf();
		if (conf == null) {
			conf = new Configuration();
			setConf(conf);
		}
		GeoWaveConfiguratorBase.setRemoteInvocationParams(
				hdfsHostPort,
				mrOptions.getResourceManagerHostPort(),
				conf);
		final QueryOptions options = new QueryOptions();
		final List<String> adapterIds = mrOptions.getAdapterIds();
		final AdapterStore adapterStore = storeOptions.createAdapterStore();
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			options.setAdapters(Lists.transform(
					adapterIds,
					new Function<String, DataAdapter<?>>() {

						@Override
						public DataAdapter<?> apply(
								final String input ) {
							return adapterStore.getAdapter(new ByteArrayId(
									input));
						}
					}));
		}
		conf.setInt(
				BATCH_SIZE_KEY,
				mrOptions.getBatchSize());
		if (mrOptions.getIndexId() != null) {
			final Index index = storeOptions.createIndexStore().getIndex(
					new ByteArrayId(
							mrOptions.getIndexId()));
			if (index == null) {
				JCommander.getConsole().println(
						"Unable to find index '" + mrOptions.getIndexId() + "' in store");
				return -1;
			}
			if (index instanceof PrimaryIndex) {
				options.setIndex((PrimaryIndex) index);
			}
			else {
				JCommander.getConsole().println(
						"Index '" + mrOptions.getIndexId() + "' is not a primary index");
				return -1;
			}
		}
		if (mrOptions.getCqlFilter() != null) {
			if ((adapterIds == null) || (adapterIds.size() != 1)) {
				JCommander.getConsole().println(
						"Exactly one type is expected when using CQL filter");
				return -1;
			}
			final String adapterId = adapterIds.get(0);
			final DataAdapter<?> adapter = storeOptions.createAdapterStore().getAdapter(
					new ByteArrayId(
							adapterId));
			if (adapter == null) {
				JCommander.getConsole().println(
						"Type '" + adapterId + "' not found");
				return -1;
			}
			if (!(adapter instanceof GeotoolsFeatureDataAdapter)) {
				JCommander.getConsole().println(
						"Type '" + adapterId + "' does not support vector export");

				return -1;
			}
			GeoWaveInputFormat.setQuery(
					conf,
					(DistributableQuery) CQLQuery.createOptimalQuery(
							mrOptions.getCqlFilter(),
							(GeotoolsFeatureDataAdapter) adapter,
							options.getIndex(),
							null));
		}
		GeoWaveInputFormat.setStoreOptions(
				conf,
				storeOptions);
		// the above code is a temporary placeholder until this gets merged with
		// the new commandline options
		GeoWaveInputFormat.setQueryOptions(
				conf,
				options);
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());

		job.setJobName("Exporting to " + hdfsPath);
		FileOutputFormat.setCompressOutput(
				job,
				true);
		FileOutputFormat.setOutputPath(
				job,
				new Path(
						hdfsPath));
		job.setMapperClass(VectorExportMapper.class);
		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		AvroJob.setOutputKeySchema(
				job,
				AvroSimpleFeatureCollection.SCHEMA$);
		AvroJob.setMapOutputKeySchema(
				job,
				AvroSimpleFeatureCollection.SCHEMA$);

		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				mrOptions.getMinSplits());
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				mrOptions.getMaxSplits());

		boolean retVal = false;
		try {
			retVal = job.waitForCompletion(true);
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Error waiting for map reduce tile resize job: ",
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
		final VectorMRExportCommand command = new VectorMRExportCommand();
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
		return runJob();
	}

}
