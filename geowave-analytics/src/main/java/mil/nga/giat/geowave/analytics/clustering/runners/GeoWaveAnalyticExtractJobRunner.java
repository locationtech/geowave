package mil.nga.giat.geowave.analytics.clustering.runners;

import java.util.Set;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.dedupe.GeoWaveDedupeJobRunner;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.clustering.mapreduce.SimpleFeatureOutputReducer;
import mil.nga.giat.geowave.analytics.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytics.parameters.ExtractParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.IndependentJobRunner;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.query.DistributableQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * Run a map reduce job to extract a population of data from GeoWave (Accumulo),
 * remove duplicates, and output a SimpleFeature with the ID and the extracted
 * geometry from each of the GeoWave data item.
 * 
 */
public class GeoWaveAnalyticExtractJobRunner extends
		GeoWaveDedupeJobRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{
	private String outputBaseDir = "/tmp";
	private int reducerCount = 1;

	public GeoWaveAnalyticExtractJobRunner() {}

	@Override
	protected int getNumReduceTasks() {
		return reducerCount;
	}

	@Override
	protected String getHdfsOutputBase() {
		return outputBaseDir;
	}

	@Override
	protected void configure(
			final Job job )
			throws Exception {

		super.configure(job);
		@SuppressWarnings("rawtypes")
		final Class<? extends DimensionExtractor> dimensionExtractorClass = job.getConfiguration().getClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS),
				SimpleFeatureGeometryExtractor.class,
				DimensionExtractor.class);

		GeoWaveOutputFormat.addDataAdapter(
				job.getConfiguration(),
				createAdapter(
						job.getConfiguration().get(
								GeoWaveConfiguratorBase.enumToConfKey(
										SimpleFeatureOutputReducer.class,
										ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID)),
						job.getConfiguration().get(
								GeoWaveConfiguratorBase.enumToConfKey(
										SimpleFeatureOutputReducer.class,
										ExtractParameters.Extract.DATA_NAMESPACE_URI)),
						dimensionExtractorClass));

		job.setJobName("GeoWave Extract (" + namespace + ")");
		job.setReduceSpeculativeExecution(false);

	}

	private FeatureDataAdapter createAdapter(
			final String outputDataTypeID,
			final String namespaceURI,
			@SuppressWarnings("rawtypes")
			final Class<? extends DimensionExtractor> dimensionExtractorClass )
			throws Exception {
		final DimensionExtractor<?> extractor = dimensionExtractorClass.newInstance();
		return AnalyticFeature.createGeometryFeatureAdapter(
				outputDataTypeID,
				extractor.getDimensionNames(),
				namespaceURI,
				ClusteringUtils.CLUSTERING_CRS);
	}

	public static Path getHdfsOutputPath(
			final PropertyManagement runTimeProperties ) {
		return new Path(
				runTimeProperties.getPropertyAsString(
						MapReduceParameters.MRConfig.HDFS_BASE_DIR,
						"/tmp") + "/" + runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_NAMESPACE,
						"x") + "_dedupe");
	}

	@Override
	public Path getHdfsOutputPath() {
		return new Path(
				getHdfsOutputBase() + "/" + namespace + "_dedupe");
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected Class<? extends Reducer> getReducer() {
		return SimpleFeatureOutputReducer.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		outputBaseDir = runTimeProperties.getPropertyAsString(
				MapReduceParameters.MRConfig.HDFS_BASE_DIR,
				"/tmp");

		LOGGER.info("Output base directory " + outputBaseDir);

		reducerCount = Math.max(
				1,
				runTimeProperties.getPropertyAsInt(
						ExtractParameters.Extract.REDUCER_COUNT,
						reducerCount));

		if (!runTimeProperties.hasProperty(ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID)) runTimeProperties.store(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				"centroid");

		RunnerUtils.setParameter(
				config,
				SimpleFeatureOutputReducer.class,
				runTimeProperties,
				new ParameterEnum[] {
					ExtractParameters.Extract.DATA_NAMESPACE_URI,
					ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID
				});

		config.set(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.GROUP_ID),
				runTimeProperties.getPropertyAsString(
						ExtractParameters.Extract.GROUP_ID,
						UUID.randomUUID().toString()));

		config.set(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						GlobalParameters.Global.BATCH_ID),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.BATCH_ID,
						UUID.randomUUID().toString()));

		DistributableQuery myQuery = query;
		if (myQuery == null) {
			myQuery = runTimeProperties.getPropertyAsQuery(ExtractParameters.Extract.QUERY);
		}
		setQuery(myQuery);
		setMinInputSplits(runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MIN_INPUT_SPLIT,
				1));
		setMaxInputSplits(runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MAX_INPUT_SPLIT,
				10000));
		setOutputBaseDir(outputBaseDir);
		setConf(config);

		config.setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS),
				runTimeProperties.getPropertyAsClass(
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
						DimensionExtractor.class,
						SimpleFeatureGeometryExtractor.class),
				DimensionExtractor.class);

		final String indexId = runTimeProperties.getPropertyAsString(ExtractParameters.Extract.INDEX_ID);
		final String adapterId = runTimeProperties.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID);

		final Index[] indices = ClusteringUtils.getIndices(runTimeProperties);

		@SuppressWarnings("rawtypes")
		final DataAdapter[] adapters = ClusteringUtils.getAdapters(runTimeProperties);

		if (adapterId != null) {
			final ByteArrayId byteId = new ByteArrayId(
					StringUtils.stringToBinary(adapterId));
			for (@SuppressWarnings("rawtypes")
			final DataAdapter adapter : adapters) {
				if (byteId.equals(adapter.getAdapterId())) {
					addDataAdapter(adapter);
				}
			}
		}

		if (indexId != null) {
			final ByteArrayId byteId = new ByteArrayId(
					StringUtils.stringToBinary(indexId));
			for (final Index index : indices) {
				if (byteId.equals(index.getId())) {
					addIndex(index);
				}
			}
		}

		return ToolRunner.run(
				config,
				this,
				runTimeProperties.toGeoWaveRunnerArguments());
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		ExtractParameters.fillOptions(
				options,
				new ExtractParameters.Extract[] {
					ExtractParameters.Extract.REDUCER_COUNT,
					ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
					ExtractParameters.Extract.DATA_NAMESPACE_URI,
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
					ExtractParameters.Extract.INDEX_ID,
					ExtractParameters.Extract.ADAPTER_ID,
					ExtractParameters.Extract.MIN_INPUT_SPLIT,
					ExtractParameters.Extract.MAX_INPUT_SPLIT,
					ExtractParameters.Extract.QUERY
				});
		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_USER,
					GlobalParameters.Global.ACCUMULO_NAMESPACE,
					GlobalParameters.Global.BATCH_ID
				});

		MapReduceParameters.fillOptions(options);

	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

	private void setOutputBaseDir(
			final String outputBaseDir ) {
		this.outputBaseDir = outputBaseDir;
	}

}