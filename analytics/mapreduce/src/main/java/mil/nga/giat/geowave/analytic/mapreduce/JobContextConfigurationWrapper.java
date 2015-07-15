package mil.nga.giat.geowave.analytic.mapreduce;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobContextConfigurationWrapper implements
		ConfigurationWrapper
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(JobContextConfigurationWrapper.class);

	private final Configuration configuration;

	private Logger logger = LOGGER;

	public JobContextConfigurationWrapper(
			final JobContext context ) {
		super();
		configuration = GeoWaveConfiguratorBase.getConfiguration(context);
	}

	public JobContextConfigurationWrapper(
			final Configuration configuration ) {
		super();
		this.configuration = configuration;
	}

	public JobContextConfigurationWrapper(
			final JobContext context,
			final Logger logger ) {
		super();
		configuration = GeoWaveConfiguratorBase.getConfiguration(context);
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
		if (configuration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		final int v = configuration.getInt(
				propName,
				defaultValue);
		return v;
	}

	@Override
	public String getString(
			final Enum<?> property,
			final Class<?> scope,
			final String defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (configuration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return configuration.get(
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
			if (configuration.getRaw(propName) == null) {
				if (defaultValue == null) {
					return null;
				}
				logger.warn("Using default for property " + propName);
			}
			return configuration.getClass(
					GeoWaveConfiguratorBase.enumToConfKey(
							scope,
							property),
					defaultValue,
					iface).newInstance();
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
			final Enum<?> property,
			final Class<?> scope,
			final double defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (configuration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return configuration.getDouble(
				propName,
				defaultValue);
	}

	@Override
	public byte[] getBytes(
			final Enum<?> property,
			final Class<?> scope ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		final String data = configuration.getRaw(propName);
		if (data == null) {
			logger.error(propName + " not found ");
		}
		return ByteArrayUtils.byteArrayFromString(data);
	}

}
