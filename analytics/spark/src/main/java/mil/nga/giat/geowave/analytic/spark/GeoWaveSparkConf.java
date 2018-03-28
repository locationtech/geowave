package mil.nga.giat.geowave.analytic.spark;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

//This class is used to create SparkConf and SparkSessions that will be compatible with GeoWave.
public class GeoWaveSparkConf implements
		Serializable
{

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
	// userConf as base. *DOES NOT MODIFY PARAMETER*
	public static SparkConf applyDefaultsToConfig(
			SparkConf userConf ) {
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
		return createDefaultSession(defaultConfig);
	}

	// Create a SparkSession with GeoWave settings and then user configuration
	// options added on top of defaults.
	public static SparkSession createDefaultSession(
			SparkConf addonOptions ) {
		SparkConf defaultConfig = GeoWaveSparkConf.getDefaultConfig();
		return SparkSession.builder().config(
				defaultConfig).config(
				addonOptions).getOrCreate();
	}

}
