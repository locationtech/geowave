package mil.nga.giat.geowave.examples.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class GeowaveRDDExample
{
	public GeowaveRDDExample() {}

	public boolean loadRddFromStore(
			final String[] args ) {
		if (args.length < 1) {
			System.err.println("Missing required arg 'storename'");
			return false;
		}

		String storeName = args[0];

		int minSplits = -1;
		int maxSplits = -1;
		DistributableQuery query = null;

		if (args.length > 1) {
			if (args[1].equals("--splits")) {
				if (args.length < 4) {
					System.err.println("USAGE: storename --splits min max");
					return false;
				}

				minSplits = Integer.parseInt(args[2]);
				maxSplits = Integer.parseInt(args[3]);

				if (args.length > 4) {
					if (args[4].equals("--bbox")) {
						if (args.length < 9) {
							System.err.println("USAGE: storename --splits min max --bbox west south east north");
							return false;
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
					return false;
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
				return false;
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

		return true;
	}
}
