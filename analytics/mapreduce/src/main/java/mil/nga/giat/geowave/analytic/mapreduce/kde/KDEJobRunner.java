package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.analytic.mapreduce.operations.KdeCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.writer.IndexWriter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class KDEJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = LoggerFactory.getLogger(KDEJobRunner.class);
	public static final String GEOWAVE_CLASSPATH_JARS = "geowave.classpath.jars";
	public static final String MAX_LEVEL_KEY = "MAX_LEVEL";
	public static final String MIN_LEVEL_KEY = "MIN_LEVEL";
	public static final String COVERAGE_NAME_KEY = "COVERAGE_NAME";
	public static final String TILE_SIZE_KEY = "TILE_SIZE";
	protected KDECommandLineOptions kdeCommandLineOptions;
	protected DataStorePluginOptions inputDataStoreOptions;
	protected DataStorePluginOptions outputDataStoreOptions;

	public KDEJobRunner(
			final KDECommandLineOptions kdeCommandLineOptions,
			final DataStorePluginOptions inputDataStoreOptions,
			final DataStorePluginOptions outputDataStoreOptions ) {
		this.kdeCommandLineOptions = kdeCommandLineOptions;
		this.inputDataStoreOptions = inputDataStoreOptions;
		this.outputDataStoreOptions = outputDataStoreOptions;
	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		Configuration conf = super.getConf();
		if (conf == null) {
			conf = new Configuration();
			setConf(conf);
		}
		GeoWaveConfiguratorBase.setRemoteInvocationParams(
				kdeCommandLineOptions.getHdfsHostPort(),
				kdeCommandLineOptions.getJobTrackerOrResourceManHostPort(),
				conf);
		conf.setInt(
				MAX_LEVEL_KEY,
				kdeCommandLineOptions.getMaxLevel());
		conf.setInt(
				MIN_LEVEL_KEY,
				kdeCommandLineOptions.getMinLevel());
		conf.set(
				COVERAGE_NAME_KEY,
				kdeCommandLineOptions.getCoverageName());
		conf.setInt(
				TILE_SIZE_KEY,
				kdeCommandLineOptions.getTileSize());
		if (kdeCommandLineOptions.getCqlFilter() != null) {
			conf.set(
					GaussianCellMapper.CQL_FILTER_KEY,
					kdeCommandLineOptions.getCqlFilter());
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
		final AdapterStore adapterStore = inputDataStoreOptions.createAdapterStore();
		final IndexStore indexStore = inputDataStoreOptions.createIndexStore();
		final QueryOptions queryOptions = new QueryOptions(
				adapterStore.getAdapter(new ByteArrayId(
						kdeCommandLineOptions.getFeatureType())));

		if (kdeCommandLineOptions.getIndexId() != null) {
			final Index index = indexStore.getIndex(new ByteArrayId(
					kdeCommandLineOptions.getIndexId()));
			if ((index != null) && (index instanceof PrimaryIndex)) {
				queryOptions.setIndex((PrimaryIndex) index);

			}
		}

		GeoWaveInputFormat.setQueryOptions(
				job.getConfiguration(),
				queryOptions);
		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				kdeCommandLineOptions.getMinSplits());
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				kdeCommandLineOptions.getMaxSplits());

		GeoWaveInputFormat.setDataStoreName(
				job.getConfiguration(),
				inputDataStoreOptions.getType());
		GeoWaveInputFormat.setStoreConfigOptions(
				job.getConfiguration(),
				inputDataStoreOptions.getFactoryOptionsAsMap());

		if (kdeCommandLineOptions.getCqlFilter() != null) {
			final Filter filter = ECQL.toFilter(kdeCommandLineOptions.getCqlFilter());
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

		final FileSystem fs = FileSystem.get(conf);
		fs.delete(
				new Path(
						"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
								+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_"
								+ kdeCommandLineOptions.getCoverageName()),
				true);
		FileOutputFormat.setOutputPath(
				job,
				new Path(
						"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
								+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_"
								+ kdeCommandLineOptions.getCoverageName() + "/basic"));

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
			statsReducer
					.setNumReduceTasks(getJob2NumReducers((kdeCommandLineOptions.getMaxLevel() - kdeCommandLineOptions
							.getMinLevel()) + 1));
			statsReducer.setMapOutputKeyClass(DoubleWritable.class);
			statsReducer.setMapOutputValueClass(LongWritable.class);
			statsReducer.setOutputKeyClass(getJob2OutputKeyClass());
			statsReducer.setOutputValueClass(getJob2OutputValueClass());
			statsReducer.setInputFormatClass(SequenceFileInputFormat.class);
			statsReducer.setOutputFormatClass(getJob2OutputFormatClass());
			FileInputFormat.setInputPaths(
					statsReducer,
					new Path(
							"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
									+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel()
									+ "_" + kdeCommandLineOptions.getCoverageName() + "/basic"));
			setupJob2Output(
					conf,
					statsReducer,
					outputDataStoreOptions.getGeowaveNamespace());
			job2Success = statsReducer.waitForCompletion(true);
			if (job2Success) {
				postJob2Success = postJob2Actions(
						conf,
						outputDataStoreOptions.getGeowaveNamespace());
			}
		}
		else {
			job2Success = false;
		}

		fs.delete(
				new Path(
						"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
								+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_"
								+ kdeCommandLineOptions.getCoverageName()),
				true);
		return (job1Success && job2Success && postJob2Success) ? 0 : 1;
	}

	protected void setupEntriesPerLevel(
			final Job job1,
			final Configuration conf )
			throws IOException {
		for (int l = kdeCommandLineOptions.getMinLevel(); l <= kdeCommandLineOptions.getMaxLevel(); l++) {
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
		return inputDataStoreOptions.getGeowaveNamespace() + "(" + kdeCommandLineOptions.getCoverageName() + ")"
				+ " levels " + kdeCommandLineOptions.getMinLevel() + "-" + kdeCommandLineOptions.getMaxLevel()
				+ " Ingest";
	}

	protected String getJob1Name() {
		return inputDataStoreOptions.getGeowaveNamespace() + "(" + kdeCommandLineOptions.getCoverageName() + ")"
				+ " levels " + kdeCommandLineOptions.getMinLevel() + "-" + kdeCommandLineOptions.getMaxLevel()
				+ " Calculation";
	}

	protected void setupJob2Output(
			final Configuration conf,
			final Job statsReducer,
			final String statsNamespace )
			throws Exception {
		final PrimaryIndex index = new SpatialIndexBuilder().createIndex();
		final WritableDataAdapter<?> adapter = RasterUtils.createDataAdapterTypeDouble(
				kdeCommandLineOptions.getCoverageName(),
				AccumuloKDEReducer.NUM_BANDS,
				kdeCommandLineOptions.getTileSize(),
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
			final PrimaryIndex index )
			throws Exception {
		GeoWaveOutputFormat.setDataStoreName(
				job.getConfiguration(),
				outputDataStoreOptions.getType());
		GeoWaveOutputFormat.setStoreConfigOptions(
				job.getConfiguration(),
				outputDataStoreOptions.getFactoryOptionsAsMap());

		GeoWaveOutputFormat.addDataAdapter(
				job.getConfiguration(),
				adapter);
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				index);
		final IndexWriter writer = outputDataStoreOptions.createDataStore().createWriter(
				adapter,
				index);
		writer.close();
	}

	public static void main(
			final String[] args )
			throws Exception {
		ConfigOptions opts = new ConfigOptions();
		OperationParser parser = new OperationParser();
		parser.addAdditionalObject(opts);
		KdeCommand command = new KdeCommand();
		CommandLineOperationParams params = parser.parse(
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
