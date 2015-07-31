package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;

public class KDEJobRunner extends
		Configured implements
		Tool
{

	public static final String GEOWAVE_CLASSPATH_JARS = "geowave.classpath.jars";
	public static final String MAX_LEVEL_KEY = "MAX_LEVEL";
	public static final String MIN_LEVEL_KEY = "MIN_LEVEL";
	public static final String COVERAGE_NAME_KEY = "COVERAGE_NAME";
	public static final String TILE_SIZE_KEY = "TILE_SIZE";
	protected String user;
	protected String password;
	protected String instance;
	protected String zookeeper;

	protected String coverageName;
	protected String namespace;
	protected String featureType;
	protected int maxLevel;
	protected int minLevel;
	protected int minSplits;
	protected int maxSplits;
	protected String cqlFilter;
	protected String newNamespace;
	protected int tileSize;

	protected String hdfsHostPort;
	protected String jobTrackerOrResourceManHostPort;

	public KDEJobRunner() {}

	public KDEJobRunner(
			final String zookeeper,
			final String instance,
			final String user,
			final String password,
			final String namespace,
			final String featureType,
			final int minLevel,
			final int maxLevel,
			final int minSplits,
			final int maxSplits,
			final String coverageName,
			final String hdfsHostPort,
			final String jobTrackerOrResourceManHostPort,
			final String newNamespace,
			final int tileSize,
			final String cqlFilter ) {
		this.zookeeper = zookeeper;
		this.instance = instance;
		this.user = user;
		this.password = password;
		this.namespace = namespace;
		this.featureType = featureType;
		this.minLevel = minLevel;
		this.maxLevel = maxLevel;
		this.minSplits = minSplits;
		this.maxSplits = maxSplits;
		this.coverageName = coverageName;
		if (!hdfsHostPort.contains("://")) {
			this.hdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		else {
			this.hdfsHostPort = hdfsHostPort;
		}
		this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
		this.newNamespace = newNamespace;
		this.tileSize = tileSize;
		this.cqlFilter = cqlFilter;
	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		final Configuration conf = super.getConf();
		GeoWaveConfiguratorBase.setRemoteInvocationParams(
				hdfsHostPort,
				jobTrackerOrResourceManHostPort,
				conf);
		conf.setInt(
				MAX_LEVEL_KEY,
				maxLevel);
		conf.setInt(
				MIN_LEVEL_KEY,
				minLevel);
		conf.set(
				COVERAGE_NAME_KEY,
				coverageName);
		conf.setInt(
				TILE_SIZE_KEY,
				tileSize);
		if (cqlFilter != null) {
			conf.set(
					GaussianCellMapper.CQL_FILTER_KEY,
					cqlFilter);
		}
		preJob1Setup(conf);
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());
		addJobClasspathDependencies(
				job,
				conf);

		job.setJobName(getJob1Name());

		job.setMapperClass(getJob1Mapper());
		job.setCombinerClass(CellSummationCombiner.class);
		job.setReducerClass(getJob1Reducer());
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(8);
		job.setSpeculativeExecution(false);
		final AccumuloOperations ops = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				namespace);
		final AdapterStore adapterStore = new AccumuloAdapterStore(
				ops);
		GeoWaveInputFormat.addDataAdapter(
				job.getConfiguration(),
				adapterStore.getAdapter(new ByteArrayId(
						featureType)));
		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				minSplits);
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				maxSplits);
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				namespace);
		if (cqlFilter != null) {
			final Filter filter = ECQL.toFilter(cqlFilter);
			final Geometry bbox = (Geometry) filter.accept(
					ExtractGeometryFilterVisitor.GEOMETRY_VISITOR,
					null);
			if ((bbox != null) && !bbox.equals(GeometryUtils.infinity())) {
				GeoWaveInputFormat.setQuery(
						job.getConfiguration(),
						new SpatialQuery(
								bbox));
			}
		}

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
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + coverageName),
				true);
		FileOutputFormat.setOutputPath(
				job,
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + coverageName + "/basic"));

		final boolean job1Success = job.waitForCompletion(true);
		boolean job2Success = false;
		boolean postJob2Success = false;
		// Linear MapReduce job chaining
		if (job1Success) {
			setupEntriesPerLevel(
					job,
					conf);
			// Stats Reducer Job configuration parameters
			final Job statsReducer = new Job(
					conf);
			statsReducer.setJarByClass(this.getClass());
			addJobClasspathDependencies(
					statsReducer,
					conf);

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
							"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + coverageName + "/basic"));
			setupJob2Output(
					conf,
					statsReducer,
					newNamespace);
			job2Success = statsReducer.waitForCompletion(true);
			if (job2Success) {
				postJob2Success = postJob2Actions(
						conf,
						newNamespace);
			}
		}
		else {
			job2Success = false;
		}

		fs.delete(
				new Path(
						"/tmp/" + namespace + "_stats_" + minLevel + "_" + maxLevel + "_" + coverageName),
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
							"level " + Long.valueOf(l)).getValue());
		}
	}

	protected void preJob1Setup(
			final Configuration conf ) {

	}

	protected boolean postJob2Actions(
			final Configuration conf,
			final String statsNamespace )
			throws Exception {
		return true;
	}

	protected Class<? extends OutputFormat<?, ?>> getJob2OutputFormatClass() {
		return GeoWaveOutputFormat.class;
	}

	protected Class<?> getJob2OutputKeyClass() {
		return GeoWaveOutputKey.class;
	}

	protected Class<?> getJob2OutputValueClass() {
		return GridCoverage.class;
	}

	protected Class<? extends Reducer<?, ?, ?, ?>> getJob2Reducer() {
		return AccumuloKDEReducer.class;
	}

	protected Class<? extends Partitioner<?, ?>> getJob2Partitioner() {
		return DoubleLevelPartitioner.class;
	}

	protected int getJob2NumReducers(
			final int numLevels ) {
		return numLevels;
	}

	protected Class<? extends Mapper<?, ?, ?, ?>> getJob1Mapper() {
		return GaussianCellMapper.class;
	}

	protected Class<? extends Reducer<?, ?, ?, ?>> getJob1Reducer() {
		return CellSummationReducer.class;
	}

	protected String getJob2Name() {
		return namespace + "(" + coverageName + ")" + " levels " + minLevel + "-" + maxLevel + " Ingest";
	}

	protected String getJob1Name() {
		return namespace + "(" + coverageName + ")" + " levels " + minLevel + "-" + maxLevel + " Calculation";
	}

	protected void setupJob2Output(
			final Configuration conf,
			final Job statsReducer,
			final String statsNamespace )
			throws Exception {
		final Index index = IndexType.SPATIAL_RASTER.createDefaultIndex();
		final WritableDataAdapter<?> adapter = RasterUtils.createDataAdapterTypeDouble(
				coverageName,
				AccumuloKDEReducer.NUM_BANDS,
				tileSize,
				AccumuloKDEReducer.MINS_PER_BAND,
				AccumuloKDEReducer.MAXES_PER_BAND,
				AccumuloKDEReducer.NAME_PER_BAND);
		setup(
				statsReducer,
				statsNamespace,
				adapter,
				index);
	}

	protected void setup(
			final Job job,
			final String namespace,
			final WritableDataAdapter<?> adapter,
			final Index index )
			throws Exception {
		final AccumuloOperations ops = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				namespace);
		GeoWaveOutputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				namespace);
		GeoWaveOutputFormat.addDataAdapter(
				job.getConfiguration(),
				adapter);
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				index);
		final DataStore store = new AccumuloDataStore(
				ops);
		final IndexWriter writer = store.createIndexWriter(index);
		writer.setupAdapter(adapter);
		writer.close();
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
		if (args.length > 0) {
			zookeeper = args[0];
			instance = args[1];
			user = args[2];
			password = args[3];
			namespace = args[4];
			featureType = args[5];
			minLevel = Integer.parseInt(args[6]);
			maxLevel = Integer.parseInt(args[7]);
			minSplits = Integer.parseInt(args[8]);
			maxSplits = Integer.parseInt(args[9]);
			coverageName = args[10];
			hdfsHostPort = args[11];
			if (!hdfsHostPort.contains("://")) {
				hdfsHostPort = "hdfs://" + hdfsHostPort;
			}
			jobTrackerOrResourceManHostPort = args[12];
			newNamespace = args[13];
			tileSize = Integer.parseInt(args[14]);
			if (args.length > getCQLFilterArg()) {
				cqlFilter = args[getCQLFilterArg()];
			}
		}
		return runJob();
	}

	protected int getCQLFilterArg() {
		return 15;
	}

	protected void addJobClasspathDependencies(
			final Job job,
			final Configuration conf )
			throws IOException,
			URISyntaxException {
		final String[] jars = conf.getTrimmedStrings(GEOWAVE_CLASSPATH_JARS);

		if (jars != null) {
			for (final String jarPath : jars) {
				job.addArchiveToClassPath(new Path(
						new URI(
								jarPath)));
			}
		}
	}
}
