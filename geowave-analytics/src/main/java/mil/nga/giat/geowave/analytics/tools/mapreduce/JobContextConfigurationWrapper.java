package mil.nga.giat.geowave.analytics.tools.mapreduce;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

public class JobContextConfigurationWrapper implements
		ConfigurationWrapper
{
	protected static final Logger LOGGER = Logger.getLogger(JobContextConfigurationWrapper.class);

	private final JobContext context;
	private Logger logger = LOGGER;

	public JobContextConfigurationWrapper(
			final JobContext context ) {
		super();
		this.context = context;
	}

	public JobContextConfigurationWrapper(
			final JobContext context,
			final Logger logger ) {
		super();
		this.context = context;
		this.logger = logger;
	}

	@Override
	public int getInt(
			final Enum<?> property,
			final Class<?> scope,
			final int defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) logger.warn("Using default for property " + propName);
		return context.getConfiguration().getInt(
				propName,
				defaultValue);
	}

	@Override
	public String getString(
			final Enum<?> property,
			final Class<?> scope,
			final String defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) logger.warn("Using default for property " + propName);
		return context.getConfiguration().get(
				propName,
				defaultValue);
	}

	@Override
	public <T> T getInstance(
			final Enum<?> property,
			final Class<?> scope,
			final Class<T> iface,
			final Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException {
		try {
			final String propName = GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property);
			if (context.getConfiguration().getRaw(
					propName) == null) logger.warn("Using default for property " + propName);
			return GeoWaveConfiguratorBase.getInstance(
					scope,
					property,
					context,
					iface,
					defaultValue);
		}
		catch (final Exception ex) {
			logger.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property));
			throw ex;
		}
	}

	@Override
	public double getDouble(
			Enum<?> property,
			Class<?> scope,
			double defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) logger.warn("Using default for property " + propName);
		return context.getConfiguration().getDouble(
				propName,
				defaultValue);
	}

	@Override
	public byte[] getBytes(
			Enum<?> property,
			Class<?> scope ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		String data = context.getConfiguration().getRaw(
				propName);
		if (data == null) logger.error(propName + " not found ");
		return ByteArrayUtils.byteArrayFromString(data);
	}

}
