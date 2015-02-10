package mil.nga.giat.geowave.analytics.spark;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.clustering.mapreduce.SimpleFeatureOutputReducer;
import mil.nga.giat.geowave.analytics.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytics.parameters.ExtractParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters.Global;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.IndependentJobRunner;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.kryo.FeatureSerializer;
import mil.nga.giat.geowave.analytics.tools.mapreduce.HadoopOptions;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.geotools.feature.simple.SimpleFeatureImpl;

import scala.Tuple2;

import com.esotericsoftware.kryo.Kryo;

public class GeowaveRDD implements
		MapReduceJobRunner,
		IndependentJobRunner,
		Serializable
{

	protected static final Logger LOGGER = Logger.getLogger(GeowaveRDD.class);

	private FeatureDataAdapter createAdapter(
			final String outputDataTypeID,
			@SuppressWarnings("rawtypes") final Class<? extends DimensionExtractor> dimensionExtractorClass )
			throws Exception {
		final DimensionExtractor<?> extractor = dimensionExtractorClass.newInstance();
		return AnalyticFeature.createGeometryFeatureAdapter(
				outputDataTypeID,
				extractor.getDimensionNames(),
				4326);
	}

	@Override
	public int run(
			Configuration config,
			PropertyManagement runTimeProperties )
			throws Exception {
		final String outputBaseDir = runTimeProperties.getProperty(
				MapReduceParameters.MRConfig.HDFS_BASE_DIR,
				"/tmp");

		LOGGER.info("Output base directory " + outputBaseDir);

		final String outputDataTypeId = runTimeProperties.getProperty(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				"centroid");

		config.set(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID),
				outputDataTypeId);

		config.set(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.GROUP_ID),
				runTimeProperties.getProperty(
						ExtractParameters.Extract.GROUP_ID,
						UUID.randomUUID().toString()));

		config.set(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						GlobalParameters.Global.BATCH_ID),
				runTimeProperties.getProperty(
						GlobalParameters.Global.BATCH_ID,
						UUID.randomUUID().toString()));

		// DistributableQuery myQuery =
		// runTimeProperties.getPropertyAsQuery(ExtractParameters.Extract.QUERY);

		// GeoWaveInputFormat.setQuery(
		// jobConf,
		// myQuery);
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				config,
				runTimeProperties.getProperty(Global.ZOOKEEKER),
				runTimeProperties.getProperty(Global.ACCUMULO_INSTANCE),
				runTimeProperties.getProperty(Global.ACCUMULO_USER),
				runTimeProperties.getProperty(Global.ACCUMULO_PASSWORD),
				runTimeProperties.getProperty(Global.ACCUMULO_NAMESPACE));

		assert (config.get(
				GeoWaveConfiguratorBase.enumToConfKey(
						GeoWaveInputFormat.class,
						GeoWaveConfiguratorBase.AccumuloOperationsConfig.ZOOKEEPER_URL),
				"").length() > 0);

		GeoWaveInputFormat.setMinimumSplitCount(
				config,
				runTimeProperties.getPropertyAsInt(
						ExtractParameters.Extract.MIN_INPUT_SPLIT,
						1));
		GeoWaveInputFormat.setMaximumSplitCount(
				config,
				runTimeProperties.getPropertyAsInt(
						ExtractParameters.Extract.MAX_INPUT_SPLIT,
						10000));

		config.setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS),
				runTimeProperties.getPropertyAsClass(
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
						SimpleFeatureGeometryExtractor.class,
						DimensionExtractor.class),
				DimensionExtractor.class);

		final String indexId = runTimeProperties.getProperty(ExtractParameters.Extract.INDEX_ID);
		final String adapterId = runTimeProperties.getProperty(ExtractParameters.Extract.ADAPTER_ID);

		final Index[] indices = ClusteringUtils.getIndices(
				runTimeProperties.getProperty(GlobalParameters.Global.ZOOKEEKER),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_INSTANCE),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_USER),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_PASSWORD),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_NAMESPACE));

		@SuppressWarnings("rawtypes")
		final DataAdapter[] adapters = ClusteringUtils.getAdapters(
				runTimeProperties.getProperty(GlobalParameters.Global.ZOOKEEKER),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_INSTANCE),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_USER),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_PASSWORD),
				runTimeProperties.getProperty(GlobalParameters.Global.ACCUMULO_NAMESPACE));

		if (adapterId != null) {
			final ByteArrayId byteId = new ByteArrayId(
					StringUtils.stringToBinary(adapterId));
			for (@SuppressWarnings("rawtypes")
			final DataAdapter adapter : adapters) {
				if (byteId.equals(adapter.getAdapterId())) {
					GeoWaveInputFormat.addDataAdapter(
							config,
							adapter);
				}
			}
		}

		if (indexId != null) {
			final ByteArrayId byteId = new ByteArrayId(
					StringUtils.stringToBinary(indexId));
			for (final Index index : indices) {
				if (byteId.equals(index.getId())) {
					GeoWaveInputFormat.addIndex(
							config,
							index);
				}
			}
		}

		doit(config);
		return 0;
	}

	public static class MyRegistrator implements
			KryoRegistrator
	{
		public void registerClasses(
				Kryo kryo ) {
			kryo.register(
					SimpleFeatureImpl.class,
					new FeatureSerializer());
		}
	}

	public void doit(
			Configuration config ) {
		GeoWaveInputFormat.setIsOutputWritable(config, true);
		
		final SparkConf conf = new SparkConf().setAppName(
				"Simple Application").setMaster(
				"local");
/*		conf.set(
				"spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		conf.set(
				"spark.kryo.registrator",
				MyRegistrator.class.getName());
	*/
		final JavaSparkContext sc = new JavaSparkContext(
				conf);

		JavaPairRDD<GeoWaveInputKey, Writable> geoData = sc.newAPIHadoopRDD(
				config,
				GeoWaveInputFormat.class,
				GeoWaveInputKey.class,
				Writable.class);

		long j = geoData.mapToPair(
				new PairFunction<Tuple2<GeoWaveInputKey, Writable>, byte[], Writable>() {
					@SuppressWarnings({
						"rawtypes",
						"unchecked"
					})
					@Override
					public Tuple2<byte[], Writable> call(
							Tuple2<GeoWaveInputKey, Writable> arg0 )
							throws Exception {
						return new Tuple2(
								arg0._1.getAdapterId().getBytes(),
								arg0._2);
					}
				}).groupByKey().count();
		System.out.println(j);
		sc.close();
	}

	@Override
	public void fillOptions(
			Set<Option> options ) {
		ExtractParameters.fillOptions(
				options,
				new ExtractParameters.Extract[] {
					ExtractParameters.Extract.REDUCER_COUNT,
					ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
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
				new HadoopOptions(
						runTimeProperties).getConfiguration(),
				runTimeProperties);
	}

}
