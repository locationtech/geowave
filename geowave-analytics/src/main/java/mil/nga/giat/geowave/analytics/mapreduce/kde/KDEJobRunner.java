package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.gt.datastore.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.BasicQuery.Constraints;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;

public class KDEJobRunner extends
		Configured implements
		Tool
{

	public static final String MAX_LEVEL_KEY = "MAX_LEVEL";
	public static final String MIN_LEVEL_KEY = "MIN_LEVEL";
	public static final String TABLE_NAME = "TABLE_NAME";

	protected String user;
	protected String password;
	protected String instance;
	protected String zookeeper;

	protected String statsName;
	protected String namespace;
	protected String featureType;
	protected int maxLevel;
	protected int minLevel;
	protected String cqlFilter;

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		final Index spatialIndex = IndexType.SPATIAL.createDefaultIndex();

		final Configuration conf = super.getConf();

		final BasicAccumuloOperations ops = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				namespace);
		final AdapterStore adapterStore = new AccumuloAdapterStore(
				ops);
		final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(featureType)));
		conf.set(
				GaussianCellMapper.DATA_ADAPTER_KEY,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(adapter)));
		conf.setInt(
				MAX_LEVEL_KEY,
				maxLevel);
		conf.setInt(
				MIN_LEVEL_KEY,
				minLevel);
		conf.set(
				AccumuloKDEReducer.STATS_NAME_KEY,
				statsName);
		if (cqlFilter != null) {
			conf.set(
					GaussianCellMapper.CQL_FILTER_KEY,
					cqlFilter);
		}
		preJob1Setup(conf);
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());

		job.setJobName(getJob1Name());

		job.setMapperClass(getJob1Mapper());
		job.setCombinerClass(CellSummationCombiner.class);
		job.setReducerClass(getJob1Reducer());
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(8);
		if (cqlFilter != null) {
			final Filter filter = ECQL.toFilter(cqlFilter);
			final Geometry bbox = (Geometry) filter.accept(
					ExtractGeometryFilterVisitor.GEOMETRY_VISITOR,
					null);
			if (bbox != null && !bbox.equals(GeometryUtils.infinity())) {
				final Constraints c = GeometryUtils.basicConstraintsFromGeometry(bbox);
				final List<ByteArrayRange> ranges = spatialIndex.getIndexStrategy().getQueryRanges(
						c.getIndexConstraints(spatialIndex.getIndexStrategy()));

				InputFormatBase.setRanges(
						job,
						AccumuloUtils.byteArrayRangesToAccumuloRanges(ranges));
			}
			conf.set(
					GaussianCellMapper.CQL_FILTER_KEY,
					cqlFilter);
		}
		
		InputFormatBase.setConnectorInfo(
				job,
				user,
				new PasswordToken(
						password.getBytes()));
		InputFormatBase.setInputTableName(
				job,
				AccumuloUtils.getQualifiedTableName(
						namespace,
						StringUtils.stringFromBinary(spatialIndex.getId().getBytes())));
		InputFormatBase.setScanAuthorizations(
				job,
				new Authorizations());

		InputFormatBase.setZooKeeperInstance(
				job,
				instance,
				zookeeper);

		// we have to at least use a whole row iterator
		final IteratorSetting iteratorSettings = new IteratorSetting(
				10,
				"GEOWAVE_WHOLE_ROW_ITERATOR",
				WholeRowIterator.class);
		InputFormatBase.addIterator(
				job,
				iteratorSettings);

		final FileSystem fs = FileSystem.get(conf);
		fs.delete(
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName),
				true);
		FileOutputFormat.setOutputPath(
				job,
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/basic"));

		final boolean job1Success = job.waitForCompletion(true);
		boolean job2Success = false;
		boolean postJob2Success = false;
		// Linear MapReduce job chaining
		if (job1Success) {
			final String statsNamespace = namespace + "_stats";
			final String tableName = AccumuloUtils.getQualifiedTableName(
					statsNamespace,
					StringUtils.stringFromBinary(spatialIndex.getId().getBytes()));

			conf.set(
					TABLE_NAME,
					tableName);
			setupEntriesPerLevel(
					job,
					conf);
			// Stats Reducer Job configuration parameters
			final Job statsReducer = new Job(
					conf);
			statsReducer.setJarByClass(this.getClass());
			statsReducer.setJobName(getJob2Name());
			statsReducer.setMapperClass(IdentityMapper.class);
			statsReducer.setPartitionerClass(getJob2Partitioner());
			statsReducer.setReducerClass(getJob2Reducer());
			statsReducer.setNumReduceTasks(getJob2NumReducers((maxLevel - minLevel) + 1));
			statsReducer.setMapOutputKeyClass(DoubleWritable.class);
			statsReducer.setMapOutputValueClass(LongWritable.class);
			statsReducer.setOutputKeyClass(getJob2OutputKeyClass());
			statsReducer.setOutputValueClass(getJob2OutputValueClass());
			statsReducer.setInputFormatClass(SequenceFileInputFormat.class);
			statsReducer.setOutputFormatClass(getJob2OutputFormatClass());
			FileInputFormat.setInputPaths(
					statsReducer,
					new Path(
							"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName + "/basic"));
			setupJob2Output(
					conf,
					spatialIndex,
					statsReducer,
					statsNamespace,
					tableName);
			job2Success = statsReducer.waitForCompletion(true);
			if (job2Success) {
				postJob2Success = postJob2Actions(
						conf,
						spatialIndex,
						statsNamespace,
						tableName);
			}
		}
		else {
			job2Success = false;
		}

		fs.delete(
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + statsName),
				true);
		return (job1Success && job2Success && postJob2Success) ? 0 : 1;

	}

	protected void setupEntriesPerLevel(
			final Job job1,
			final Configuration conf )
			throws IOException {
		for (int l = minLevel; l <= maxLevel; l++) {
			conf.setLong(
					"Entries per level.level" + l,
					job1.getCounters().getGroup(
							"Entries per level").findCounter(
							"level " + new Long(
									l)).getValue());
		}
	}

	protected void preJob1Setup(
			final Configuration conf ) {

	}

	protected boolean postJob2Actions(
			final Configuration conf,
			final Index spatialIndex,
			final String statsNamespace,
			final String tableName )
			throws Exception {
		return true;
	}

	protected Class getJob2OutputFormatClass() {
		return AccumuloOutputFormat.class;
	}

	protected Class getJob2OutputKeyClass() {
		return Text.class;
	}

	protected Class getJob2OutputValueClass() {
		return Mutation.class;
	}

	protected Class getJob2Reducer() {
		return AccumuloKDEReducer.class;
	}

	protected Class getJob2Partitioner() {
		return DoubleLevelPartitioner.class;
	}

	protected int getJob2NumReducers(
			final int numLevels ) {
		return numLevels;
	}

	protected Class getJob1Mapper() {
		return GaussianCellMapper.class;
	}

	protected Class getJob1Reducer() {
		return CellSummationReducer.class;
	}

	protected String getJob2Name() {
		return namespace + "(" + statsName + ")" + " levels " + minLevel + "-" + maxLevel + " Ingest";
	}

	protected String getJob1Name() {
		return namespace + "(" + statsName + ")" + " levels " + minLevel + "-" + maxLevel + " Calculation";
	}

	protected void setupJob2Output(
			final Configuration conf,
			final Index spatialIndex,
			final Job statsReducer,
			final String statsNamespace,
			final String tableName )
			throws Exception {
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
					AccumuloKDEReducer.createFeatureType(AccumuloKDEReducer.getTypeName(
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
				statsReducer,
				instance,
				zookeeper);
		AccumuloOutputFormat.setCreateTables(
				statsReducer,
				true);
		AccumuloOutputFormat.setConnectorInfo(
				statsReducer,
				user,
				new PasswordToken(
						password.getBytes()));
		AccumuloOutputFormat.setDefaultTableName(
				statsReducer,
				tableName);
	}

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new KDEJobRunner(),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		zookeeper = args[0];
		instance = args[1];
		user = args[2];
		password = args[3];
		namespace = args[4];
		featureType = args[5];
		minLevel = Integer.parseInt(args[6]);
		maxLevel = Integer.parseInt(args[7]);
		statsName = args[8];
		if (args.length > getCQLFilterArg()) {
			cqlFilter = args[getCQLFilterArg()];
		}
		return runJob();
	}

	protected int getCQLFilterArg() {
		return 9;
	}

}
