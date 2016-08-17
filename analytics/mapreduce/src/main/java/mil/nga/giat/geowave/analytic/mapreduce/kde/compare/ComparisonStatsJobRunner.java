package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.analytic.mapreduce.kde.KDECommandLineOptions;
import mil.nga.giat.geowave.analytic.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.plugins.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class ComparisonStatsJobRunner extends
		KDEJobRunner
{
	private final String timeAttribute;

	public ComparisonStatsJobRunner(
			ComparisonCommandLineOptions inputOptions,
			KDECommandLineOptions kdeCommandLineOptions,
			DataStorePluginOptions inputDataStoreOptions,
			DataStorePluginOptions outputDataStoreOptions ) {
		super(
				kdeCommandLineOptions,
				inputDataStoreOptions,
				outputDataStoreOptions);
		timeAttribute = inputOptions.getTimeAttribute();
	}

	@Override
	protected void preJob1Setup(
			final Configuration conf ) {
		super.preJob1Setup(conf);
		conf.set(
				ComparisonGaussianCellMapper.TIME_ATTRIBUTE_KEY,
				timeAttribute);
	}

	@Override
	protected boolean postJob2Actions(
			final Configuration conf,
			final String statsNamespace )
			throws Exception {
		final FileSystem fs = FileSystem.get(conf);
		fs.delete(
				new Path(
						"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
								+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_"
								+ kdeCommandLineOptions.getCoverageName() + "/basic"),
				true);
		final Job combiner = new Job(
				conf);
		combiner.setJarByClass(this.getClass());
		combiner.setJobName(inputDataStoreOptions.getGeowaveNamespace() + "(" + kdeCommandLineOptions.getCoverageName()
				+ ")" + " levels " + kdeCommandLineOptions.getMinLevel() + "-" + kdeCommandLineOptions.getMaxLevel()
				+ " combining seasons");
		combiner.setMapperClass(ComparisonCombiningStatsMapper.class);
		combiner.setReducerClass(ComparisonCombiningStatsReducer.class);
		combiner.setMapOutputKeyClass(LongWritable.class);
		combiner.setMapOutputValueClass(DoubleWritable.class);
		combiner.setOutputKeyClass(ComparisonCellData.class);
		combiner.setOutputValueClass(LongWritable.class);
		combiner.setInputFormatClass(SequenceFileInputFormat.class);
		combiner.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(
				combiner,
				new Path(
						"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
								+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_"
								+ kdeCommandLineOptions.getCoverageName() + "/combined_pct"));

		FileInputFormat.setInputPaths(
				combiner,
				new Path(
						"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
								+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_"
								+ kdeCommandLineOptions.getCoverageName() + "/percentiles"));
		if (combiner.waitForCompletion(true)) {

			fs.delete(
					new Path(
							"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
									+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel()
									+ "_" + kdeCommandLineOptions.getCoverageName() + "/percentiles"),
					true);
			for (int l = kdeCommandLineOptions.getMinLevel(); l <= kdeCommandLineOptions.getMaxLevel(); l++) {
				conf.setLong(
						"Entries per level.level" + l,
						combiner.getCounters().getGroup(
								"Entries per level").findCounter(
								"level " + Long.valueOf(l)).getValue());
			}
			// Stats Reducer Job configuration parameters
			final Job ingester = new Job(
					conf);
			ingester.setJarByClass(this.getClass());
			ingester.setJobName(inputDataStoreOptions.getGeowaveNamespace() + "("
					+ kdeCommandLineOptions.getCoverageName() + ")" + " levels " + kdeCommandLineOptions.getMinLevel()
					+ "-" + kdeCommandLineOptions + " Ingest");
			ingester.setMapperClass(ComparisonIdentityMapper.class);
			ingester.setPartitionerClass(ComparisonCellLevelPartitioner.class);
			ingester.setReducerClass(ComparisonAccumuloStatsReducer.class);
			ingester.setNumReduceTasks((kdeCommandLineOptions.getMaxLevel() - kdeCommandLineOptions.getMinLevel()) + 1);
			ingester.setMapOutputKeyClass(ComparisonCellData.class);
			ingester.setMapOutputValueClass(LongWritable.class);
			ingester.setOutputKeyClass(GeoWaveOutputKey.class);
			ingester.setOutputValueClass(SimpleFeature.class);
			ingester.setInputFormatClass(SequenceFileInputFormat.class);
			ingester.setOutputFormatClass(GeoWaveOutputFormat.class);

			FileInputFormat.setInputPaths(
					ingester,
					new Path(
							"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
									+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel()
									+ "_" + kdeCommandLineOptions.getCoverageName() + "/combined_pct"));
			GeoWaveOutputFormat.setDataStoreName(
					conf,
					outputDataStoreOptions.getType());
			GeoWaveOutputFormat.setStoreConfigOptions(
					conf,
					outputDataStoreOptions.getFactoryOptionsAsMap());

			setup(
					ingester,
					statsNamespace,
					RasterUtils.createDataAdapterTypeDouble(
							statsNamespace,
							ComparisonAccumuloStatsReducer.NUM_BANDS,
							kdeCommandLineOptions.getTileSize()),
					new SpatialDimensionalityTypeProvider().createPrimaryIndex());
			return ingester.waitForCompletion(true);

		}
		return false;
	}

	@Override
	protected Class getJob2OutputFormatClass() {
		return SequenceFileOutputFormat.class;
	}

	@Override
	protected Class getJob2OutputKeyClass() {
		return LongWritable.class;
	}

	@Override
	protected Class getJob2OutputValueClass() {
		return DoubleWritable.class;
	}

	@Override
	protected Class getJob2Reducer() {
		return ComparisonCellDataReducer.class;
	}

	@Override
	protected int getJob2NumReducers(
			final int numLevels ) {
		return super.getJob2NumReducers(numLevels) * 2;
	}

	@Override
	protected Class getJob1Mapper() {
		return ComparisonGaussianCellMapper.class;
	}

	@Override
	protected Class getJob1Reducer() {
		return ComparisonCellSummationReducer.class;
	}

	@Override
	protected Class getJob2Partitioner() {
		return ComparisonDoubleLevelPartitioner.class;
	}

	@Override
	protected String getJob2Name() {
		return inputDataStoreOptions.getGeowaveNamespace() + "(" + kdeCommandLineOptions.getCoverageName() + ")"
				+ " levels " + kdeCommandLineOptions.getMinLevel() + "-" + kdeCommandLineOptions.getMaxLevel()
				+ " Percentile Calculation by season";
	}

	@Override
	protected String getJob1Name() {
		return super.getJob1Name() + " initial calculation by season";
	}

	@Override
	protected void setupEntriesPerLevel(
			final Job job1,
			final Configuration conf )
			throws IOException {
		for (int l = kdeCommandLineOptions.getMinLevel(); l <= kdeCommandLineOptions.getMaxLevel(); l++) {
			conf.setLong(
					"Entries per level (winter, " + l + ")",
					job1.getCounters().getGroup(
							"Entries per level (winter)").findCounter(
							"level " + Long.valueOf(l)).getValue());
			conf.setLong(
					"Entries per level (summer, " + l + ")",
					job1.getCounters().getGroup(
							"Entries per level (summer)").findCounter(
							"level " + Long.valueOf(l)).getValue());
		}
	}

	@Override
	protected void setupJob2Output(
			final Configuration conf,
			final Job statsReducer,
			final String statsNamespace )
			throws Exception {
		FileOutputFormat.setOutputPath(
				statsReducer,
				new Path(
						"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
								+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_"
								+ kdeCommandLineOptions.getCoverageName() + "/percentiles"));
	}

}
