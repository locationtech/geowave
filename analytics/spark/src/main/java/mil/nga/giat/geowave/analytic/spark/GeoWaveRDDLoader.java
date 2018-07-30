package mil.nga.giat.geowave.analytic.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;

public class GeoWaveRDDLoader
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveRDDLoader.class);

	public static GeoWaveRDD loadRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions )
			throws IOException {
		RDDOptions defaultOptions = new RDDOptions();
		return GeoWaveRDDLoader.loadRDD(
				sc,
				storeOptions,
				defaultOptions);
	}

	public static GeoWaveRDD loadRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions,
			final RDDOptions rddOpts )
			throws IOException {
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD = GeoWaveRDDLoader.loadRawRDD(
				sc,
				storeOptions,
				rddOpts);
		return new GeoWaveRDD(
				rawRDD);
	}

	public static GeoWaveIndexedRDD loadIndexedRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions,
			final RDDOptions rddOpts,
			final NumericIndexStrategy indexStrategy )
			throws IOException {
		GeoWaveRDD wrappedRDD = GeoWaveRDDLoader.loadRDD(
				sc,
				storeOptions,
				rddOpts);
		if (wrappedRDD == null) {
			return null;
		}
		// Index strategy can be expensive so we will broadcast it and store it
		Broadcast<NumericIndexStrategy> broadcastStrategy = null;
		if (indexStrategy != null) {
			broadcastStrategy = (Broadcast<NumericIndexStrategy>) RDDUtils.broadcastIndexStrategy(
					sc,
					indexStrategy);
		}

		GeoWaveIndexedRDD returnRDD = new GeoWaveIndexedRDD(
				wrappedRDD,
				broadcastStrategy);
		return returnRDD;
	}

	public static GeoWaveIndexedRDD loadIndexedRDD(
			final SparkContext sc,
			final GeoWaveRDD inputRDD,
			final NumericIndexStrategy indexStrategy )
			throws IOException {
		if (inputRDD == null || !inputRDD.isLoaded()) {
			return null;
		}
		// Index strategy can be expensive so we will broadcast it and store it
		Broadcast<NumericIndexStrategy> broadcastStrategy = null;
		if (indexStrategy != null) {
			broadcastStrategy = (Broadcast<NumericIndexStrategy>) RDDUtils.broadcastIndexStrategy(
					sc,
					indexStrategy);
		}

		GeoWaveIndexedRDD returnRDD = new GeoWaveIndexedRDD(
				inputRDD,
				broadcastStrategy);
		return returnRDD;
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> loadRawRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions,
			final RDDOptions rddOpts )
			throws IOException {
		if (sc == null) {
			LOGGER.error("Must supply a valid Spark Context. Please set SparkContext and try again.");
			return null;
		}

		if (storeOptions == null) {
			LOGGER.error("Must supply input store to load. Please set storeOptions and try again.");
			return null;
		}

		if (rddOpts == null) {
			LOGGER.error("Must supply valid RDDOptions to load a rdd.");
			return null;
		}

		Configuration conf = new Configuration(
				sc.hadoopConfiguration());

		GeoWaveInputFormat.setStoreOptions(
				conf,
				storeOptions);

		if (rddOpts.getQuery() != null) {
			GeoWaveInputFormat.setQuery(
					conf,
					rddOpts.getQuery());
		}

		if (rddOpts.getQueryOptions() != null) {
			GeoWaveInputFormat.setQueryOptions(
					conf,
					rddOpts.getQueryOptions());
		}

		if (rddOpts.getMinSplits() > -1 || rddOpts.getMaxSplits() > -1) {
			GeoWaveInputFormat.setMinimumSplitCount(
					conf,
					rddOpts.getMinSplits());
			GeoWaveInputFormat.setMaximumSplitCount(
					conf,
					rddOpts.getMaxSplits());
		}
		else {
			int defaultSplitsSpark = sc.getConf().getInt(
					"spark.default.parallelism",
					-1);
			// Attempt to grab default partition count for spark and split data
			// along that.
			// Otherwise just fallback to default according to index strategy
			if (defaultSplitsSpark != -1) {
				GeoWaveInputFormat.setMinimumSplitCount(
						conf,
						defaultSplitsSpark);
				GeoWaveInputFormat.setMaximumSplitCount(
						conf,
						defaultSplitsSpark);
			}
		}

		RDD<Tuple2<GeoWaveInputKey, SimpleFeature>> rdd = sc.newAPIHadoopRDD(
				conf,
				GeoWaveInputFormat.class,
				GeoWaveInputKey.class,
				SimpleFeature.class);

		JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = JavaPairRDD.fromJavaRDD(rdd.toJavaRDD());

		return javaRdd;
	}

}
