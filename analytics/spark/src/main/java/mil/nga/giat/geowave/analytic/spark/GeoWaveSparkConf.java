package mil.nga.giat.geowave.analytic.spark;

import java.io.Serializable;

import mil.nga.giat.geowave.analytic.spark.sparksql.GeoWaveSpatialEncoders;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//This class is used to create SparkConf and SparkSessions that will be compatible with GeoWave.
public class GeoWaveSparkConf implements
		Serializable
{

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkConf.class);

	// Returns a SparkConf with just the basic settings necessary for spark to
	// work with GeoWave
	public static SparkConf getDefaultConfig() {
		SparkConf defaultConfig = new SparkConf();
		defaultConfig = defaultConfig.setMaster("yarn");
		defaultConfig = defaultConfig.set(
				"spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		defaultConfig = defaultConfig.set(
				"spark.kryo.registrator",
				"mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator");
		return defaultConfig;
	}

	// Returns a *NEW* SparkConf with GeoWave default settings applied using
	// userConf as base.
	public static SparkConf applyDefaultsToConfig(
			final SparkConf userConf ) {
		SparkConf newConf = userConf.clone();
		newConf = newConf.set(
				"spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		newConf = newConf.set(
				"spark.kryo.registrator",
				"mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator");
		return newConf;
	}

	// Create a default SparkSession with GeoWave settings applied to config.
	public static SparkSession createDefaultSession() {
		SparkConf defaultConfig = GeoWaveSparkConf.getDefaultConfig();
		return GeoWaveSparkConf.internalCreateSession(
				defaultConfig,
				null);
	}

	// Create a SparkSession with GeoWave settings and then user configuration
	// options added on top of defaults.
	public static SparkSession createDefaultSession(
			final SparkConf addonOptions ) {
		SparkConf defaultConfig = GeoWaveSparkConf.getDefaultConfig();
		return GeoWaveSparkConf.internalCreateSession(
				defaultConfig,
				addonOptions);
	}

	// Create a SparkSession from default config with additional options, if
	// set. Mainly used from Command line runners.
	public static SparkSession createSessionFromParams(
			String appName,
			String master,
			String host,
			String jars ) {
		// Grab default config for GeoWave
		SparkConf defaultConfig = GeoWaveSparkConf.getDefaultConfig();
		// Apply master from default
		if (master == null) {
			master = "yarn";
		}

		// Apply user options if set, correctly handling host for yarn.
		if (appName != null) {
			defaultConfig = defaultConfig.setAppName(appName);
		}
		if (master != null) {
			defaultConfig = defaultConfig.setMaster(master);
		}
		if (host != null) {
			if (master != null && master != "yarn") {
				defaultConfig = defaultConfig.set(
						"spark.driver.host",
						host);
			}
			else {
				LOGGER
						.warn("Attempting to set spark driver host for yarn master. Normally this is handled via hadoop configuration. Remove host or set another master designation and try again.");
			}
		}

		if (jars != null) {
			defaultConfig = defaultConfig.set(
					"spark.jars",
					jars);
		}

		// Finally return the session from builder
		return GeoWaveSparkConf.internalCreateSession(
				defaultConfig,
				null);
	}

	private static SparkSession internalCreateSession(
			SparkConf conf,
			SparkConf addonOptions ) {

		// Create initial SessionBuilder from default Configuration.
		Builder builder = SparkSession.builder().config(
				conf);

		// Ensure SpatialEncoders and UDTs are registered at each session
		// creation.
		GeoWaveSpatialEncoders.registerUDTs();

		if (addonOptions != null) {
			builder = builder.config(addonOptions);
		}

		return builder.getOrCreate();
	}

}
