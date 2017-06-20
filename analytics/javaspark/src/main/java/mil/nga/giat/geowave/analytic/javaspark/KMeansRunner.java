package mil.nga.giat.geowave.analytic.javaspark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class KMeansRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KMeansRunner.class);

	private final JavaSparkContext jsc;
	private DataStorePluginOptions inputDataStore = null;
	private KMeansModel outputModel;

	private int numClusters = 8;
	private int numIterations = 20;

	public KMeansRunner() {
		this(
				null,
				null);
	}

	public KMeansRunner(
			String master ) {
		this(
				null,
				master);
	}

	public KMeansRunner(
			String appName,
			String master ) {
		if (appName == null) {
			appName = "KMeansRunner";
		}

		if (master == null) {
			master = "local";
		}

		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName(appName);
		sparkConf.setMaster(master);

		jsc = new JavaSparkContext(
				sparkConf);
	}

	public void run()
			throws IOException {
		// Validate inputs
		if (inputDataStore == null) {
			LOGGER.error("You must supply an input datastore!");
			throw new IOException(
					"You must supply an input datastore!");
		}

		// Load RDD from datastore
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaPairRdd = GeoWaveRDD.rddForSimpleFeatures(
				jsc.sc(),
				inputDataStore);

		// Retrieve the input centroids
		JavaRDD<Vector> centroidVectors = GeoWaveRDD.rddFeatureVectors(javaPairRdd);
		centroidVectors.cache();

		// Run KMeans
		outputModel = KMeans.train(
				centroidVectors.rdd(),
				numClusters,
				numIterations);
	}

	public DataStorePluginOptions getInputDataStore() {
		return inputDataStore;
	}

	public void setInputDataStore(
			DataStorePluginOptions inputDataStore ) {
		this.inputDataStore = inputDataStore;
	}

	public void setNumClusters(
			int numClusters ) {
		this.numClusters = numClusters;
	}

	public void setNumIterations(
			int numIterations ) {
		this.numIterations = numIterations;
	}

	public KMeansModel getOutputModel() {
		return outputModel;
	}
}
