package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class SpatialJoinRunner implements
		Serializable
{

	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialJoinRunner.class);

	// Options provided by user to run join
	private SpatialJoinOptions options = null;
	private SparkSession session = null;

	// Variables loaded during runner. This can be updated to something cleaner
	// like GeoWaveRDD in future
	// to support different situations (indexed vs non indexed etc..) but keep
	// it hidden in implementation details
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD;
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD;

	// TODO: Join strategy could be supplied as variable or determined
	// automatically from index store (would require associating index and join
	// strategy)
	// for now will just use TieredSpatialJoin as that is the only one we have
	// implemented.
	private JoinStrategy joinStrategy = new TieredSpatialJoin();

	public SpatialJoinRunner() {}

	public SpatialJoinRunner(
			SpatialJoinOptions opts,
			SparkSession session ) {
		this.options = opts;
		this.session = session;
	}

	public void run()
			throws InterruptedException,
			ExecutionException,
			IOException {
		// Verify CRS match/transform possible
		verifyCRS();
		// Init context
		initContext();
		// Load RDDs
		loadDatasets();
		// Run join
		joinStrategy.join(
				session,
				leftRDD,
				rightRDD,
				this.options.getPredicate(),
				this.options.getIndexStrategy());

		writeResultsToNewAdapter();
	}

	private void writeResultsToNewAdapter() {
		// TODO: Write each result set new adapterId
	}

	private void initContext() {

		if (session == null) {
			session = SparkSession.builder().appName(
					"SpatialJoin").master(
					"local[*]").config(
					"spark.serializer",
					"org.apache.spark.serializer.KryoSerializer").config(
					"spark.kryo.registrator",
					"mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator").getOrCreate();
		}

		// TODO: Verify all necessary options are set to perform join from spark

	}

	private void loadDatasets()
			throws IOException {

		leftRDD = GeoWaveRDD.rddForSimpleFeatures(
				this.session.sparkContext(),
				this.options.getLeftStore());

		rightRDD = GeoWaveRDD.rddForSimpleFeatures(
				this.session.sparkContext(),
				this.options.getRightStore());

	}

	private void verifyCRS() {
		// TODO: Verify that both stores have matching CRS or that one CRS can
		// be transformed into the other
	}

	public SpatialJoinOptions getOptions() {
		return options;
	}

	public void setOptions(
			SpatialJoinOptions options ) {
		this.options = options;
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getLeftResults() {
		return this.joinStrategy.getLeftResults();
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getRightResults() {
		return this.joinStrategy.getRightResults();
	}

}