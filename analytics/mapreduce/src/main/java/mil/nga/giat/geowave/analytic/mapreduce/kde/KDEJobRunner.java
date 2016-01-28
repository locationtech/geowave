package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

public class KDEJobRunner extends
		Configured implements
		Tool,
		CLIOperationDriver
{
	private static final Logger LOGGER = LoggerFactory.getLogger(KDEJobRunner.class);
	public static final String GEOWAVE_CLASSPATH_JARS = "geowave.classpath.jars";
	public static final String MAX_LEVEL_KEY = "MAX_LEVEL";
	public static final String MIN_LEVEL_KEY = "MIN_LEVEL";
	public static final String COVERAGE_NAME_KEY = "COVERAGE_NAME";
	public static final String TILE_SIZE_KEY = "TILE_SIZE";
	protected KDECommandLineOptions kdeCommandLineOptions;
	protected DataStoreCommandLineOptions inputDataStoreOptions;
	protected DataStoreCommandLineOptions outputDataStoreOptions;
	protected AdapterStoreCommandLineOptions inputAdapterStoreOptions;

	public KDEJobRunner() {}

	public KDEJobRunner(
			final KDECommandLineOptions kdeCommandLineOptions,
			final DataStoreCommandLineOptions inputDataStoreOptions,
			final DataStoreCommandLineOptions outputDataStoreOptions ) {
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
		final AdapterStore adapterStore = inputAdapterStoreOptions.createStore();
		GeoWaveInputFormat.addDataAdapter(
				job.getConfiguration(),
				adapterStore.getAdapter(new ByteArrayId(
						kdeCommandLineOptions.getFeatureType())));
		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				kdeCommandLineOptions.getMinSplits());
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				kdeCommandLineOptions.getMaxSplits());

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
						"/tmp/" + inputDataStoreOptions.getNamespace() + "_stats_" + kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_" + kdeCommandLineOptions.getCoverageName()),
				true);
		FileOutputFormat.setOutputPath(
				job,
				new Path(
						"/tmp/" + inputDataStoreOptions.getNamespace() + "_stats_" + kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_" + kdeCommandLineOptions.getCoverageName() + "/basic"));

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
			statsReducer.setNumReduceTasks(getJob2NumReducers((kdeCommandLineOptions.getMaxLevel() - kdeCommandLineOptions.getMinLevel()) + 1));
			statsReducer.setMapOutputKeyClass(DoubleWritable.class);
			statsReducer.setMapOutputValueClass(LongWritable.class);
			statsReducer.setOutputKeyClass(getJob2OutputKeyClass());
			statsReducer.setOutputValueClass(getJob2OutputValueClass());
			statsReducer.setInputFormatClass(SequenceFileInputFormat.class);
			statsReducer.setOutputFormatClass(getJob2OutputFormatClass());
			FileInputFormat.setInputPaths(
					statsReducer,
					new Path(
							"/tmp/" + inputDataStoreOptions.getNamespace() + "_stats_" + kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_" + kdeCommandLineOptions.getCoverageName() + "/basic"));
			setupJob2Output(
					conf,
					statsReducer,
					outputDataStoreOptions.getNamespace());
			job2Success = statsReducer.waitForCompletion(true);
			if (job2Success) {
				postJob2Success = postJob2Actions(
						conf,
						outputDataStoreOptions.getNamespace());
			}
		}
		else {
			job2Success = false;
		}

		fs.delete(
				new Path(
						"/tmp/" + inputDataStoreOptions.getNamespace() + "_stats_" + kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel() + "_" + kdeCommandLineOptions.getCoverageName()),
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
		return inputDataStoreOptions.getNamespace() + "(" + kdeCommandLineOptions.getCoverageName() + ")" + " levels " + kdeCommandLineOptions.getMinLevel() + "-" + kdeCommandLineOptions.getMaxLevel() + " Ingest";
	}

	protected String getJob1Name() {
		return inputDataStoreOptions.getNamespace() + "(" + kdeCommandLineOptions.getCoverageName() + ")" + " levels " + kdeCommandLineOptions.getMinLevel() + "-" + kdeCommandLineOptions.getMaxLevel() + " Calculation";
	}

	protected void setupJob2Output(
			final Configuration conf,
			final Job statsReducer,
			final String statsNamespace )
			throws Exception {
		final PrimaryIndex index = new SpatialIndexBuilder().setAllTiers(
				true).createIndex();
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
				outputDataStoreOptions.getFactory().getName());
		GeoWaveOutputFormat.setStoreConfigOptions(
				job.getConfiguration(),
				ConfigUtils.valuesToStrings(
						outputDataStoreOptions.getConfigOptions(),
						outputDataStoreOptions.getFactory().getOptions()));
		GeoWaveOutputFormat.setGeoWaveNamespace(
				job.getConfiguration(),
				outputDataStoreOptions.getNamespace());
		GeoWaveOutputFormat.addDataAdapter(
				job.getConfiguration(),
				adapter);
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				index);
		final IndexWriter writer = outputDataStoreOptions.createStore().createIndexWriter(
				index,
				DataStoreUtils.DEFAULT_VISIBILITY);
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

	protected void applyOptions(
			final Options allOptions ) {
		DataStoreCommandLineOptions.applyOptions(
				"input_",
				allOptions);
		DataStoreCommandLineOptions.applyOptions(
				"output_",
				allOptions);

		AdapterStoreCommandLineOptions.applyOptions(
				"input_",
				allOptions);
		KDECommandLineOptions.applyOptions(allOptions);
	}

	protected CommandLine parseOptions(
			final String[] args,
			final Options allOptions )
			throws Exception {
		final BasicParser parser = new BasicParser();
		CommandLine commandLine = parser.parse(
				allOptions,
				args,
				true);
		boolean newCommandLine = false;
		CommandLineResult<DataStoreCommandLineOptions> inputDataStoreOptionsResult = null;
		CommandLineResult<DataStoreCommandLineOptions> outputDataStoreOptionsResult = null;
		CommandLineResult<AdapterStoreCommandLineOptions> inputAdapterStoreOptionsResult = null;
		Exception parseException = null;
		do {
			inputDataStoreOptionsResult = null;
			outputDataStoreOptionsResult = null;
			inputAdapterStoreOptionsResult = null;
			parseException = null;
			newCommandLine = false;
			try {
				kdeCommandLineOptions = KDECommandLineOptions.parseOptions(commandLine);
			}
			catch (final Exception e) {
				parseException = e;
			}

			try {
				inputDataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
						"input_",
						allOptions,
						commandLine);
			}
			catch (final Exception e) {
				parseException = e;
			}
			try {
				inputAdapterStoreOptionsResult = AdapterStoreCommandLineOptions.parseOptions(
						"input_",
						allOptions,
						commandLine);
			}
			catch (final Exception e) {
				parseException = e;
			}
			if ((inputAdapterStoreOptionsResult != null) && inputAdapterStoreOptionsResult.isCommandLineChange()) {
				commandLine = inputAdapterStoreOptionsResult.getCommandLine();
				// newCommandLine = true;
				// continue;
			}

			if ((inputDataStoreOptionsResult != null) && inputDataStoreOptionsResult.isCommandLineChange()) {
				commandLine = inputDataStoreOptionsResult.getCommandLine();
			}
			try {
				outputDataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
						"output_",
						allOptions,
						commandLine);
			}
			catch (final Exception e) {
				parseException = e;
			}
			if ((outputDataStoreOptionsResult != null) && outputDataStoreOptionsResult.isCommandLineChange()) {
				commandLine = outputDataStoreOptionsResult.getCommandLine();
				// newCommandLine = true;
				// continue;
			}

		}
		while (newCommandLine);
		if (parseException != null) {
			throw parseException;
		}
		inputDataStoreOptions = inputDataStoreOptionsResult.getResult();
		outputDataStoreOptions = outputDataStoreOptionsResult.getResult();
		inputAdapterStoreOptions = inputAdapterStoreOptionsResult.getResult();
		return commandLine;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Options allOptions = new Options();
		applyOptions(allOptions);
		parseOptions(
				args,
				allOptions);
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
