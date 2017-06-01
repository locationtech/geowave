package mil.nga.giat.geowave.analytic.javaspark;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
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
	private static Logger LOGGER = LoggerFactory.getLogger(
			GeoWaveRDD.class);
	
	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> rddForSimpleFeatures(
			SparkContext sc,
			Map<String, String> storeOptions,
			DistributableQuery query,
			QueryOptions queryOptions )
			throws IOException {

		Configuration conf = new Configuration(
				sc.hadoopConfiguration());

		GeoWaveInputFormat.setStoreOptionsMap(
				conf,
				storeOptions);

		GeoWaveInputFormat.setQuery(
				conf,
				query);

		GeoWaveInputFormat.setQueryOptions(
				conf,
				queryOptions);

		RDD<Tuple2<GeoWaveInputKey, SimpleFeature>> rdd = sc.newAPIHadoopRDD(
				conf,
				GeoWaveInputFormat.class,
				GeoWaveInputKey.class,
				SimpleFeature.class);

		JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = JavaPairRDD.fromRDD(
				rdd,
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
						GeoWaveInputKey.class),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
						SimpleFeature.class));

		return javaRdd;
	}

	public static void main(
			final String[] args ) {
		if (args.length < 1) {
			System.err.println("Missing required arg 'storename'");
			System.exit(-1);
		}

		String storeName = args[0];

		double west = -180.0;
		double south = -90.0;
		double east = 180.0;
		double north = 90.0;
				
		if (args.length > 1) {
			if  (args.length < 5) {
				System.err.println("USAGE: storename west, south, east, north");
			}
			
			west = Double.parseDouble(args[1]);
			south = Double.parseDouble(args[2]);
			east = Double.parseDouble(args[3]);
			north = Double.parseDouble(args[4]);
		}

		try {
			DataStorePluginOptions inputStoreOptions = null;
			// Attempt to load input store.
			if (inputStoreOptions == null) {
				final StoreLoader inputStoreLoader = new StoreLoader(
						storeName);
				if (!inputStoreLoader.loadFromConfig(
						ConfigOptions.getDefaultPropertyFile())) {
					throw new IOException(
							"Cannot find store name: " + inputStoreLoader.getStoreName());
				}
				inputStoreOptions = inputStoreLoader.getDataStorePlugin();
			}
			
			SparkConf sparkConf = new SparkConf();

			sparkConf.setAppName(
					"GeoWaveRDD");
			sparkConf.setMaster(
					"local");
			JavaSparkContext context = new JavaSparkContext(
					sparkConf);

			Geometry bbox = new GeometryFactory().toGeometry(
					new Envelope(
							west,
							south,
							east,
							north));

			SpatialQuery query = new SpatialQuery(
					bbox);

			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					inputStoreOptions.getOptionsAsMap(),
					query,
					new QueryOptions());

			System.out.println(
					"DataStore " + storeName + " loaded into RDD with " + javaRdd.count() + " features.");

		}
		catch (IOException e) {
			System.err.println(
					e.getMessage());
		}
	}
}
