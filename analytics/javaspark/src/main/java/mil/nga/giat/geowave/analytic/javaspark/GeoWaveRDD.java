package mil.nga.giat.geowave.analytic.javaspark;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.store.query.ScaledTemporalRange;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class GeoWaveRDD
{
	private static Logger LOGGER = LoggerFactory.getLogger(GeoWaveRDD.class);

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions )
			throws IOException {
		return rddForSimpleFeatures(
				sc,
				storeOptions,
				null,
				null,
				-1,
				-1);
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions,
			DistributableQuery query )
			throws IOException {
		return rddForSimpleFeatures(
				sc,
				storeOptions,
				query,
				new QueryOptions(),
				-1,
				-1);
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions,
			DistributableQuery query,
			QueryOptions queryOptions )
			throws IOException {
		return rddForSimpleFeatures(
				sc,
				storeOptions,
				query,
				queryOptions,
				-1,
				-1);
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			DataStorePluginOptions storeOptions,
			DistributableQuery query,
			QueryOptions queryOptions,
			int minSplits,
			int maxSplits )
			throws IOException {
		Configuration conf = new Configuration(
				sc.hadoopConfiguration());

		GeoWaveInputFormat.setStoreOptions(
				conf,
				storeOptions);

		if (query != null) {
			GeoWaveInputFormat.setQuery(
					conf,
					query);
		}

		if (queryOptions != null) {
			GeoWaveInputFormat.setQueryOptions(
					conf,
					queryOptions);
		}

		if (minSplits > -1) {
			GeoWaveInputFormat.setMinimumSplitCount(
					conf,
					minSplits);
			GeoWaveInputFormat.setMaximumSplitCount(
					conf,
					maxSplits);
		}

		RDD<Tuple2<GeoWaveInputKey, SimpleFeature>> rdd = sc.newAPIHadoopRDD(
				conf,
				GeoWaveInputFormat.class,
				GeoWaveInputKey.class,
				SimpleFeature.class);

		JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = JavaPairRDD.fromRDD(
				rdd,
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(GeoWaveInputKey.class),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(SimpleFeature.class));

		return javaRdd;
	}

	public static JavaRDD<SimpleFeature> rddFeatures(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		return pairRDD.values();
	}

	public static JavaRDD<Vector> rddFeatureVectors(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		return rddFeatureVectors(
				pairRDD,
				null,
				null);
	}

	public static JavaRDD<Vector> rddFeatureVectors(
			final JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD,
			final String timeField,
			final ScaledTemporalRange scaledRange) {
		JavaRDD<Vector> vectorRDD = pairRDD.values().map(
				feature -> {
					Point centroid = ((Geometry) feature.getDefaultGeometry()).getCentroid();
					
					int numValues = 2;
					Date time = null;
					
					if (timeField != null) {
						time = (Date) feature.getAttribute(timeField);

						if (time != null) {
							numValues++;
						}
					}

					double[] values = new double[numValues];
					values[0] = centroid.getX();
					values[1] = centroid.getY();
					
					if (time != null) {						
						values[2] = scaledRange.timeToValue(time);
					}

					return Vectors.dense(
							values);
				});

		return vectorRDD;
	}

	public static JavaRDD<Point> rddFeatureCentroids(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		JavaRDD<Point> centroids = pairRDD.values().map(
				feature -> {
					Geometry geom = (Geometry) feature.getDefaultGeometry();
					return geom.getCentroid();
				});

		return centroids;
	}

	public static void main(
			final String[] args ) {
		if (args.length < 1) {
			System.err.println("Missing required arg 'storename'");
			System.exit(-1);
		}

		String storeName = args[0];

		int minSplits = -1;
		int maxSplits = -1;
		DistributableQuery query = null;

		if (args.length > 1) {
			if (args[1].equals("--splits")) {
				if (args.length < 4) {
					System.err.println("USAGE: storename --splits min max");
					System.exit(-1);
				}

				minSplits = Integer.parseInt(args[2]);
				maxSplits = Integer.parseInt(args[3]);

				if (args.length > 4) {
					if (args[4].equals("--bbox")) {
						if (args.length < 9) {
							System.err.println("USAGE: storename --splits min max --bbox west south east north");
							System.exit(-1);
						}

						double west = Double.parseDouble(args[5]);
						double south = Double.parseDouble(args[6]);
						double east = Double.parseDouble(args[7]);
						double north = Double.parseDouble(args[8]);

						Geometry bbox = new GeometryFactory().toGeometry(new Envelope(
								west,
								south,
								east,
								north));

						query = new SpatialQuery(
								bbox);
					}
				}
			}
			else if (args[1].equals("--bbox")) {
				if (args.length < 6) {
					System.err.println("USAGE: storename --bbox west south east north");
					System.exit(-1);
				}

				double west = Double.parseDouble(args[2]);
				double south = Double.parseDouble(args[3]);
				double east = Double.parseDouble(args[4]);
				double north = Double.parseDouble(args[5]);

				Geometry bbox = new GeometryFactory().toGeometry(new Envelope(
						west,
						south,
						east,
						north));

				query = new SpatialQuery(
						bbox);
			}
			else {
				System.err.println("USAGE: storename --splits min max --bbox west south east north");
				System.exit(-1);
			}
		}

		try {
			DataStorePluginOptions inputStoreOptions = null;
			// Attempt to load input store.
			if (inputStoreOptions == null) {
				final StoreLoader inputStoreLoader = new StoreLoader(
						storeName);
				if (!inputStoreLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
					throw new IOException(
							"Cannot find store name: " + inputStoreLoader.getStoreName());
				}
				inputStoreOptions = inputStoreLoader.getDataStorePlugin();
			}

			SparkConf sparkConf = new SparkConf();

			sparkConf.setAppName("GeoWaveRDD");
			sparkConf.setMaster("local");
			JavaSparkContext context = new JavaSparkContext(
					sparkConf);

			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					inputStoreOptions,
					query,
					null,
					minSplits,
					maxSplits);

			System.out.println("DataStore " + storeName + " loaded into RDD with " + javaRdd.count() + " features.");

			context.close();
		}
		catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}
}
