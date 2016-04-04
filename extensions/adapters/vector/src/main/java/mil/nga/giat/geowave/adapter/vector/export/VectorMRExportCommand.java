package mil.nga.giat.geowave.adapter.vector.export;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

public class VectorMRExportCommand extends
		Configured implements
		Tool,
		CLIOperationDriver
{
	// TODO annotate appropriately when new commandline tools is merged
	private static final Logger LOGGER = Logger.getLogger(VectorMRExportCommand.class);

	public static final String BATCH_SIZE_KEY = "BATCH_SIZE";
	private VectorMRExportOptions mrOptions;

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
				mrOptions.getHdfsHostPort(),
				mrOptions.getResourceManagerHostPort(),
				conf);
		final QueryOptions options = new QueryOptions();
		final List<String> adapterIds = mrOptions.getAdapterIds();
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			options.setAdapterIds(Lists.transform(
					adapterIds,
					new Function<String, ByteArrayId>() {

						@Override
						public ByteArrayId apply(
								final String input ) {
							return new ByteArrayId(
									input);
						}
					}));
			try (CloseableIterator<DataAdapter<?>> it = options.getAdapters(mrOptions.getAdapterStore())) {
				while (it.hasNext()) {
					final DataAdapter<?> adapter = it.next();
					JobContextAdapterStore.addDataAdapter(
							conf,
							adapter);
				}
			}
		}
		conf.setInt(
				BATCH_SIZE_KEY,
				mrOptions.getBatchSize());
		if (mrOptions.getIndexId() != null) {
			final Index index = mrOptions.getIndexStore().getIndex(
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
			final DataAdapter<?> adapter = mrOptions.getAdapterStore().getAdapter(
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

					new CQLQuery(
							mrOptions.getCqlFilter(),
							(GeotoolsFeatureDataAdapter) adapter));
		}
		// TODO set data store appropriately for input format
		GeoWaveInputFormat.setQueryOptions(
				conf,
				options);
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());

		job.setJobName("Exporting to " + mrOptions.getHdfsOutputFile());
		FileOutputFormat.setCompressOutput(
				job,
				true);
		AvroJob.setOutputKeySchema(
				job,
				AvroSimpleFeatureCollection.SCHEMA$);
		job.setMapperClass(VectorExportMapper.class);
		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);

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
		final int res = ToolRunner.run(
				new Configuration(),
				new VectorMRExportCommand(),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
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
