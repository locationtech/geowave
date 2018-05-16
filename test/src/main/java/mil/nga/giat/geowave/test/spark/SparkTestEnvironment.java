package mil.nga.giat.geowave.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.spark.GeoWaveSparkConf;
import mil.nga.giat.geowave.test.TestEnvironment;

public class SparkTestEnvironment implements
		TestEnvironment
{

	private static final Logger LOGGER = LoggerFactory.getLogger(SparkTestEnvironment.class);

	private static SparkTestEnvironment singletonInstance = null;
	protected SparkSession defaultSession = null;
	protected SparkContext defaultContext = null;

	public static synchronized SparkTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new SparkTestEnvironment();
		}
		return singletonInstance;
	}

	@Override
	public void setup()
			throws Exception {
		if (defaultSession == null) {
			SparkConf addonOptions = new SparkConf();
			addonOptions.setMaster("local[*]");
			addonOptions.setAppName("CoreGeoWaveSparkITs");
			defaultSession = GeoWaveSparkConf.createDefaultSession(addonOptions);
			if (defaultSession == null) {
				LOGGER.error("Unable to create default spark session for tests");
				return;
			}
			defaultContext = defaultSession.sparkContext();
		}
	}

	@Override
	public void tearDown()
			throws Exception {
		if (defaultSession != null) {
			defaultContext = null;
			defaultSession.close();
			defaultSession = null;
		}
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

	public SparkSession getDefaultSession() {
		return defaultSession;
	}

	public SparkContext getDefaultContext() {
		return defaultContext;
	}

}
