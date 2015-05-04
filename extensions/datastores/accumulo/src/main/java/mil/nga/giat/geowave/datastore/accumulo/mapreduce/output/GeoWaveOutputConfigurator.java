package mil.nga.giat.geowave.datastore.accumulo.mapreduce.output;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * This class defines the configuration for GeoWave output format.
 */
public class GeoWaveOutputConfigurator extends
		GeoWaveConfiguratorBase
{
	/**
	 * General configuration keys
	 * 
	 */
	public static Boolean canCreateIndex(
			final Class<?> implementingClass,
			final JobContext context ) {
		return canCreateIndexInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static Boolean canCreateTables(
			final Class<?> implementingClass,
			final JobContext context ) {
		return canCreateTablesInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setCreateTables(
			final Class<?> implementingClass,
			final Job job,
			final boolean enableFeature ) {
		job.getConfiguration().setBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_TABLES),
				enableFeature);
	}

	public static void setCreateIndex(
			final Class<?> implementingClass,
			final Job job,
			final boolean enableFeature ) {
		job.getConfiguration().setBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_INDEX),
				enableFeature);
	}

	private static Boolean canCreateIndexInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.getBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_INDEX),
				true);

	}

	private static Boolean canCreateTablesInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.getBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_TABLES),
				true);

	}
}
