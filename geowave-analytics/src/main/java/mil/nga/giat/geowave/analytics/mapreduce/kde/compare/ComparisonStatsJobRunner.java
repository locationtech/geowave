package mil.nga.giat.geowave.analytics.mapreduce.kde.compare;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.analytics.mapreduce.kde.AccumuloKDEReducer;
import mil.nga.giat.geowave.analytics.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class ComparisonStatsJobRunner extends
		KDEJobRunner
{
	private String timeAttribute;

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new ComparisonStatsJobRunner(),
				args);
		System.exit(res);
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
			final Index spatialIndex,
			final String statsNamespace,
			final String tableName )
			throws Exception {
		final FileSystem fs = FileSystem.get(conf);
		fs.delete(
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/basic"),
				true);
		final Job combiner = new Job(
				conf);
		combiner.setJarByClass(this.getClass());
		combiner.setJobName(namespace + "(" + statsName + ")" + " levels " + minLevel + "-" + maxLevel + " combining seasons");
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
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/combined_pct"));

		FileInputFormat.setInputPaths(
				combiner,
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/percentiles"));
		if (combiner.waitForCompletion(true)) {

			fs.delete(
					new Path(
							"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/percentiles"),
					true);
			for (int l = minLevel; l <= maxLevel; l++) {
				conf.setLong(
						"Entries per level.level" + l,
						combiner.getCounters().getGroup(
								"Entries per level").findCounter(
								"level " + new Long(
										l)).getValue());
			}
			// Stats Reducer Job configuration parameters
			final Job ingester = new Job(
					conf);
			ingester.setJarByClass(this.getClass());
			ingester.setJobName(namespace + "(" + statsName + ")" + " levels " + minLevel + "-" + maxLevel + " Ingest");
			ingester.setMapperClass(ComparisonIdentityMapper.class);
			ingester.setPartitionerClass(ComparisonCellLevelPartitioner.class);
			ingester.setReducerClass(ComparisonAccumuloStatsReducer.class);
			ingester.setNumReduceTasks((maxLevel - minLevel) + 1);
			ingester.setMapOutputKeyClass(ComparisonCellData.class);
			ingester.setMapOutputValueClass(LongWritable.class);
			ingester.setOutputKeyClass(Text.class);
			ingester.setOutputValueClass(Mutation.class);
			ingester.setInputFormatClass(SequenceFileInputFormat.class);
			ingester.setOutputFormatClass(AccumuloOutputFormat.class);

			FileInputFormat.setInputPaths(
					ingester,
					new Path(
							"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/combined_pct"));
			final BasicAccumuloOperations statsOperations = new BasicAccumuloOperations(
					zookeeper,
					instance,
					user,
					password,
					statsNamespace);
			final AccumuloAdapterStore statsAdapterStore = new AccumuloAdapterStore(
					statsOperations);
			for (int level = minLevel; level <= maxLevel; level++) {
				final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
						ComparisonAccumuloStatsReducer.createFeatureType(AccumuloKDEReducer.getTypeName(
								level,
								statsName)));
				if (!statsAdapterStore.adapterExists(featureAdapter.getAdapterId())) {
					statsAdapterStore.addAdapter(featureAdapter);
				}
			}
			final AccumuloIndexStore statsIndexStore = new AccumuloIndexStore(
					statsOperations);
			if (!statsIndexStore.indexExists(spatialIndex.getId())) {
				statsIndexStore.addIndex(spatialIndex);
			}
			AccumuloOutputFormat.setZooKeeperInstance(
					ingester,
					instance,
					zookeeper);
			AccumuloOutputFormat.setCreateTables(
					ingester,
					true);
			AccumuloOutputFormat.setConnectorInfo(
					ingester,
					user,
					new PasswordToken(
							password.getBytes()));
			AccumuloOutputFormat.setDefaultTableName(
					ingester,
					tableName);
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
		return namespace + "(" + statsName + ")" + " levels " + minLevel + "-" + maxLevel + " Percentile Calculation by season";
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
		for (int l = minLevel; l <= maxLevel; l++) {
			conf.setLong(
					"Entries per level (winter, " + l + ")",
					job1.getCounters().getGroup(
							"Entries per level (winter)").findCounter(
							"level " + new Long(
									l)).getValue());
			conf.setLong(
					"Entries per level (summer, " + l + ")",
					job1.getCounters().getGroup(
							"Entries per level (summer)").findCounter(
							"level " + new Long(
									l)).getValue());
		}
	}

	@Override
	protected void setupJob2Output(
			final Configuration conf,
			final Index spatialIndex,
			final Job statsReducer,
			final String statsNamespace,
			final String tableName )
			throws Exception {
		FileOutputFormat.setOutputPath(
				statsReducer,
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/percentiles"));
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		timeAttribute = args[super.getCQLFilterArg()];
		return super.run(args);
	}

	@Override
	protected int getCQLFilterArg() {
		return super.getCQLFilterArg() + 1;
	}

}
