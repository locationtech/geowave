/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.geotools.referencing.CRS;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.operations.ResizeCommand;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitorResult;
import mil.nga.giat.geowave.analytic.mapreduce.operations.KdeCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.ClearCommand;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class KDEJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = LoggerFactory.getLogger(KDEJobRunner.class);
	public static final String GEOWAVE_CLASSPATH_JARS = "geowave.classpath.jars";
	private static final String TMP_COVERAGE_SUFFIX = "_tMp_CoVeRaGe";
	protected static int TILE_SIZE = 1;
	public static final String MAX_LEVEL_KEY = "MAX_LEVEL";
	public static final String MIN_LEVEL_KEY = "MIN_LEVEL";
	public static final String COVERAGE_NAME_KEY = "COVERAGE_NAME";
	protected KDECommandLineOptions kdeCommandLineOptions;
	protected DataStorePluginOptions inputDataStoreOptions;
	protected DataStorePluginOptions outputDataStoreOptions;
	protected File configFile;
	protected PrimaryIndex outputIndex;
	public static final String X_MIN_KEY = "X_MIN";
	public static final String X_MAX_KEY = "X_MAX";
	public static final String Y_MIN_KEY = "Y_MIN";
	public static final String Y_MAX_KEY = "Y_MAX";
	public static final String INPUT_CRSCODE_KEY = "INPUT_CRS";
	public static final String OUTPUT_CRSCODE_KEY = "OUTPUT_CRS";

	public KDEJobRunner(
			final KDECommandLineOptions kdeCommandLineOptions,
			final DataStorePluginOptions inputDataStoreOptions,
			final DataStorePluginOptions outputDataStoreOptions,
			final File configFile,
			final PrimaryIndex outputIndex ) {
		this.kdeCommandLineOptions = kdeCommandLineOptions;
		this.inputDataStoreOptions = inputDataStoreOptions;
		this.outputDataStoreOptions = outputDataStoreOptions;
		this.configFile = configFile;
		this.outputIndex = outputIndex;
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

		PrimaryIndex inputPrimaryIndex = null;
		final CloseableIterator<Index<?, ?>> it1 = inputDataStoreOptions.createIndexStore().getIndices();
		while (it1.hasNext()) {
			Index<?, ?> index = it1.next();
			if (index instanceof PrimaryIndex) {
				inputPrimaryIndex = (PrimaryIndex) index;
				break;
			}
		}

		CoordinateReferenceSystem inputIndexCrs = GeometryUtils.getIndexCrs(inputPrimaryIndex);
		String inputCrsCode = GeometryUtils.getCrsCode(inputIndexCrs);

		PrimaryIndex outputPrimaryIndex = outputIndex;
		CoordinateReferenceSystem outputIndexCrs = null;
		String outputCrsCode = null;

		if (outputPrimaryIndex != null) {
			outputIndexCrs = GeometryUtils.getIndexCrs(outputPrimaryIndex);
			outputCrsCode = GeometryUtils.getCrsCode(outputIndexCrs);
		}
		else {
			SpatialDimensionalityTypeProvider sdp = new SpatialDimensionalityTypeProvider();
			SpatialOptions so = sdp.createOptions();
			so.setCrs(inputCrsCode);
			outputPrimaryIndex = sdp.createPrimaryIndex(so);
			outputIndexCrs = inputIndexCrs;
			outputCrsCode = inputCrsCode;
		}

		CoordinateSystem cs = outputIndexCrs.getCoordinateSystem();
		CoordinateSystemAxis csx = cs.getAxis(0);
		CoordinateSystemAxis csy = cs.getAxis(1);
		double xMax = csx.getMaximumValue();
		double xMin = csx.getMinimumValue();
		double yMax = csy.getMaximumValue();
		double yMin = csy.getMinimumValue();

		if (xMax == Double.POSITIVE_INFINITY || xMin == Double.NEGATIVE_INFINITY || yMax == Double.POSITIVE_INFINITY
				|| yMin == Double.NEGATIVE_INFINITY) {
			LOGGER
					.error("Raster KDE resize with raster primary index CRS dimensions min/max equal to positive infinity or negative infinity is not supported");
			throw new RuntimeException(
					"Raster KDE resize with raster primary index CRS dimensions min/max equal to positive infinity or negative infinity is not supported");
		}

		DataStorePluginOptions rasterResizeOutputDataStoreOptions;
		String kdeCoverageName;
		// so we don't need a no data merge strategy, use 1 for the tile size of
		// the KDE output and then run a resize operation
		if ((kdeCommandLineOptions.getTileSize() > 1)) {
			// this is the ending data store options after resize, the KDE will
			// need to output to a temporary namespace, a resize operation
			// will use the outputDataStoreOptions
			rasterResizeOutputDataStoreOptions = outputDataStoreOptions;

			// first clone the outputDataStoreOptions, then set it to a tmp
			// namespace
			final Map<String, String> configOptions = outputDataStoreOptions.getOptionsAsMap();
			final StoreFactoryOptions options = ConfigUtils.populateOptionsFromList(
					outputDataStoreOptions.getFactoryFamily().getDataStoreFactory().createOptionsInstance(),
					configOptions);
			options.setGeowaveNamespace(outputDataStoreOptions.getGeowaveNamespace() + "_tmp");
			outputDataStoreOptions = new DataStorePluginOptions(
					options);
			kdeCoverageName = kdeCommandLineOptions.getCoverageName() + TMP_COVERAGE_SUFFIX;
		}
		else {
			rasterResizeOutputDataStoreOptions = null;
			kdeCoverageName = kdeCommandLineOptions.getCoverageName();
		}

		if (kdeCommandLineOptions.getHdfsHostPort() == null) {
			Properties configProperties = ConfigOptions.loadProperties(configFile);
			String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
			kdeCommandLineOptions.setHdfsHostPort(hdfsFSUrl);
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
				kdeCoverageName);
		if (kdeCommandLineOptions.getCqlFilter() != null) {
			conf.set(
					GaussianCellMapper.CQL_FILTER_KEY,
					kdeCommandLineOptions.getCqlFilter());
		}
		conf.setDouble(
				X_MIN_KEY,
				xMin);
		conf.setDouble(
				X_MAX_KEY,
				xMax);
		conf.setDouble(
				Y_MIN_KEY,
				yMin);
		conf.setDouble(
				Y_MAX_KEY,
				yMax);
		conf.set(
				INPUT_CRSCODE_KEY,
				inputCrsCode);
		conf.set(
				OUTPUT_CRSCODE_KEY,
				outputCrsCode);

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
		final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				kdeCommandLineOptions.getFeatureType()));
		final QueryOptions queryOptions = new QueryOptions(
				adapter);

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

		GeoWaveInputFormat.setStoreOptions(
				job.getConfiguration(),
				inputDataStoreOptions);

		if (kdeCommandLineOptions.getCqlFilter() != null) {
			Geometry bbox = null;
			if (adapter instanceof FeatureDataAdapter) {
				final String geometryAttribute = ((FeatureDataAdapter) adapter)
						.getFeatureType()
						.getGeometryDescriptor()
						.getLocalName();
				final Filter filter = ECQL.toFilter(kdeCommandLineOptions.getCqlFilter());
				final ExtractGeometryFilterVisitorResult geoAndCompareOpData = (ExtractGeometryFilterVisitorResult) filter
						.accept(
								new ExtractGeometryFilterVisitor(
										GeometryUtils.DEFAULT_CRS,
										geometryAttribute),
								null);
				bbox = geoAndCompareOpData.getGeometry();
			}

			if ((bbox != null) && !bbox.equals(GeometryUtils.infinity())) {
				GeoWaveInputFormat.setQuery(
						job.getConfiguration(),
						new SpatialQuery(
								bbox));
			}
		}

		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			fs.delete(
					new Path(
							"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
									+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel()
									+ "_" + kdeCommandLineOptions.getCoverageName()),
					true);
			FileOutputFormat.setOutputPath(
					job,
					new Path(
							"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
									+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel()
									+ "_" + kdeCommandLineOptions.getCoverageName() + "/basic"));

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
										+ kdeCommandLineOptions.getMinLevel() + "_"
										+ kdeCommandLineOptions.getMaxLevel() + "_"
										+ kdeCommandLineOptions.getCoverageName() + "/basic"));
				setupJob2Output(
						conf,
						statsReducer,
						outputDataStoreOptions.getGeowaveNamespace(),
						kdeCoverageName,
						outputPrimaryIndex);
				job2Success = statsReducer.waitForCompletion(true);
				if (job2Success) {
					postJob2Success = postJob2Actions(
							conf,
							outputDataStoreOptions.getGeowaveNamespace(),
							kdeCoverageName);
				}
			}
			else {
				job2Success = false;
			}

			if (rasterResizeOutputDataStoreOptions != null) {
				// delegate to resize command to wrap it up with the correctly
				// requested tile size

				final ResizeCommand resizeCommand = new ResizeCommand();

				// We're going to override these anyway.
				resizeCommand.setParameters(
						null,
						null);

				resizeCommand.setInputStoreOptions(outputDataStoreOptions);
				resizeCommand.setOutputStoreOptions(rasterResizeOutputDataStoreOptions);

				resizeCommand.getOptions().setInputCoverageName(
						kdeCoverageName);
				resizeCommand.getOptions().setMinSplits(
						kdeCommandLineOptions.getMinSplits());
				resizeCommand.getOptions().setMaxSplits(
						kdeCommandLineOptions.getMaxSplits());
				resizeCommand.getOptions().setHdfsHostPort(
						kdeCommandLineOptions.getHdfsHostPort());
				resizeCommand.getOptions().setJobTrackerOrResourceManHostPort(
						kdeCommandLineOptions.getJobTrackerOrResourceManHostPort());
				resizeCommand.getOptions().setOutputCoverageName(
						kdeCommandLineOptions.getCoverageName());

				resizeCommand.getOptions().setOutputTileSize(
						kdeCommandLineOptions.getTileSize());

				final int resizeStatus = ToolRunner.run(
						resizeCommand.createRunner(new ManualOperationParams()),
						new String[] {});
				if (resizeStatus == 0) {
					// delegate to clear command to clean up with tmp namespace
					// after successful resize
					final ClearCommand clearCommand = new ClearCommand();
					clearCommand.setParameters(null);
					clearCommand.setInputStoreOptions(outputDataStoreOptions);
					clearCommand.execute(new ManualOperationParams());
				}
				else {
					LOGGER.warn("Resize command error code '" + resizeStatus + "'.  Retaining temporary namespace '"
							+ outputDataStoreOptions.getGeowaveNamespace() + "' with tile size of 1.");
				}
			}

			fs.delete(
					new Path(
							"/tmp/" + inputDataStoreOptions.getGeowaveNamespace() + "_stats_"
									+ kdeCommandLineOptions.getMinLevel() + "_" + kdeCommandLineOptions.getMaxLevel()
									+ "_" + kdeCommandLineOptions.getCoverageName()),
					true);
			return (job1Success && job2Success && postJob2Success) ? 0 : 1;
		}
		finally {
			if (fs != null) {
				fs.close();
			}
		}
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
			final String statsNamespace,
			final String coverageName )
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
			final String statsNamespace,
			final String coverageName,
			final PrimaryIndex index )
			throws Exception {
		final WritableDataAdapter<?> adapter = RasterUtils.createDataAdapterTypeDouble(
				coverageName,
				AccumuloKDEReducer.NUM_BANDS,
				TILE_SIZE,
				AccumuloKDEReducer.MINS_PER_BAND,
				AccumuloKDEReducer.MAXES_PER_BAND,
				AccumuloKDEReducer.NAME_PER_BAND,
				null);
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
			throws IOException,
			MismatchedIndexToAdapterMapping {
		GeoWaveOutputFormat.setStoreOptions(
				job.getConfiguration(),
				outputDataStoreOptions);

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
		final ConfigOptions opts = new ConfigOptions();
		final OperationParser parser = new OperationParser();
		parser.addAdditionalObject(opts);
		final KdeCommand command = new KdeCommand();
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
