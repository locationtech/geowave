package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

public class GeoWaveOutputConfigurator extends
		GeoWaveConfiguratorBase
{
	/**
	 * General configuration keys
	 * 
	 */
	// TODO use these options to restrict creation when set (currently, within
	// the GeoWavevDataStore if anything doesn't exist it is automatically
	// persisted)
	protected static enum GeneralConfig {
		CREATE_ADAPTERS,
		CREATE_INDEX,
		CREATE_TABLES
	}

	public static Boolean canCreateAdapters(
			final Class<?> implementingClass,
			final JobContext context ) {
		return canCreateAdaptersInternal(
				implementingClass,
				getConfiguration(context));
	}

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

	public static void setCreateAdapters(
			final Class<?> implementingClass,
			final Job job,
			final boolean enableFeature ) {
		job.getConfiguration().setBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_ADAPTERS),
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

	private static Boolean canCreateAdaptersInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.getBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_ADAPTERS),
				true);
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
