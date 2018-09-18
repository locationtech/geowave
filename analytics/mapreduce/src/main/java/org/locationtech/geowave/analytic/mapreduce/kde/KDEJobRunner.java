/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic.mapreduce.kde;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.operations.ResizeCommand;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.mapreduce.operations.KdeCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitor;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitorResult;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.config.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.remote.ClearCommand;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.config.ConfigUtils;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

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
	protected Index outputIndex;
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
			final Index outputIndex ) {
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

		Index inputPrimaryIndex = null;
		final Index[] idxArray = inputDataStoreOptions.createDataStore().getIndices();
		for (Index idx : idxArray) {
			if (idx != null) {
				inputPrimaryIndex = idx;
				break;
			}
		}

		final CoordinateReferenceSystem inputIndexCrs = GeometryUtils.getIndexCrs(inputPrimaryIndex);
		final String inputCrsCode = GeometryUtils.getCrsCode(inputIndexCrs);

		Index outputPrimaryIndex = outputIndex;
		CoordinateReferenceSystem outputIndexCrs = null;
		String outputCrsCode = null;

		if (outputPrimaryIndex != null) {
			outputIndexCrs = GeometryUtils.getIndexCrs(outputPrimaryIndex);
			outputCrsCode = GeometryUtils.getCrsCode(outputIndexCrs);
		}
		else {
			final SpatialDimensionalityTypeProvider sdp = new SpatialDimensionalityTypeProvider();
			final SpatialOptions so = sdp.createOptions();
			so.setCrs(inputCrsCode);
			outputPrimaryIndex = sdp.createIndex(so);
			outputIndexCrs = inputIndexCrs;
			outputCrsCode = inputCrsCode;
		}

		final CoordinateSystem cs = outputIndexCrs.getCoordinateSystem();
		final CoordinateSystemAxis csx = cs.getAxis(0);
		final CoordinateSystemAxis csy = cs.getAxis(1);
		final double xMax = csx.getMaximumValue();
		final double xMin = csx.getMinimumValue();
		final double yMax = csy.getMaximumValue();
		final double yMin = csy.getMinimumValue();

		if ((xMax == Double.POSITIVE_INFINITY) || (xMin == Double.NEGATIVE_INFINITY)
				|| (yMax == Double.POSITIVE_INFINITY) || (yMin == Double.NEGATIVE_INFINITY)) {
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
			final Properties configProperties = ConfigOptions.loadProperties(configFile);
			final String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
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
		final PersistentAdapterStore adapterStore = inputDataStoreOptions.createAdapterStore();
		final IndexStore indexStore = inputDataStoreOptions.createIndexStore();
		final InternalAdapterStore internalAdapterStore = inputDataStoreOptions.createInternalAdapterStore();
		final short internalAdapterId = internalAdapterStore.getAdapterId(

		kdeCommandLineOptions.getFeatureType());
		final DataTypeAdapter<?> adapter = adapterStore.getAdapter(
				internalAdapterId).getAdapter();

		VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName());
		if (kdeCommandLineOptions.getIndexName() != null) {
			bldr = bldr.indexName(kdeCommandLineOptions.getIndexName());
		}

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
										GeometryUtils.getDefaultCRS(),
										geometryAttribute),
								null);
				bbox = geoAndCompareOpData.getGeometry();
			}

			if ((bbox != null) && !bbox.equals(GeometryUtils.infinity())) {
				bldr = bldr.constraints(bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
						bbox).build());
			}
		}
		GeoWaveInputFormat.setQuery(
				conf,
				bldr.build(),
				adapterStore,
				internalAdapterStore,
				indexStore);
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
				final File configFile = File.createTempFile(
						"temp-config",
						null);
				final ManualOperationParams params = new ManualOperationParams();

				params.getContext().put(
						ConfigOptions.PROPERTIES_FILE_CONTEXT,
						configFile);
				final AddStoreCommand addStore = new AddStoreCommand();
				addStore.setParameters("temp-out");
				addStore.setPluginOptions(outputDataStoreOptions);
				addStore.execute(params);
				addStore.setParameters("temp-raster-out");
				addStore.setPluginOptions(rasterResizeOutputDataStoreOptions);
				addStore.execute(params);
				// We're going to override these anyway.
				resizeCommand.setParameters(
						"temp-out",
						"temp-raster-out");

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
						resizeCommand.createRunner(params),
						new String[] {});
				if (resizeStatus == 0) {
					// delegate to clear command to clean up with tmp namespace
					// after successful resize
					final ClearCommand clearCommand = new ClearCommand();
					clearCommand.setParameters("temp-out");
					clearCommand.execute(params);
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
				try {
					fs.close();
				}
				catch (final IOException e) {
					LOGGER.info(e.getMessage());
					// Attempt to close, but don't throw an error if it is
					// already closed.
					// Log message, so find bugs does not complain.
				}
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
		return KDEReducer.class;
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
			final Index index )
			throws Exception {
		final DataTypeAdapter<?> adapter = RasterUtils.createDataAdapterTypeDouble(
				coverageName,
				KDEReducer.NUM_BANDS,
				TILE_SIZE,
				KDEReducer.MINS_PER_BAND,
				KDEReducer.MAXES_PER_BAND,
				KDEReducer.NAME_PER_BAND,
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
			final DataTypeAdapter<?> adapter,
			final Index index )
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
		final DataStore dataStore = outputDataStoreOptions.createDataStore();
		dataStore.addType(
				adapter,
				index);
		final Writer writer = dataStore.createWriter(adapter.getTypeName());
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
