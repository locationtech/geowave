package mil.nga.giat.geowave.analytic;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScopedJobConfiguration
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(ScopedJobConfiguration.class);

	private final Configuration jobConfiguration;

	private final Class<?> scope;
	private Logger logger = LOGGER;

	public ScopedJobConfiguration(
			final Configuration jobConfiguration,
			final Class<?> scope ) {
		super();
		this.jobConfiguration = jobConfiguration;
		this.scope = scope;
	}

	public ScopedJobConfiguration(
			final Configuration jobConfiguration,
			final Class<?> scope,
			final Logger logger ) {
		super();
		this.jobConfiguration = jobConfiguration;
		this.scope = scope;
		this.logger = logger;
	}

	public int getInt(
			final Enum<?> property,
			final int defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (jobConfiguration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		final int v = jobConfiguration.getInt(
				propName,
				defaultValue);
		return v;
	}

	public String getString(
			final Enum<?> property,
			final String defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (jobConfiguration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return jobConfiguration.get(
				propName,
				defaultValue);
	}

	public <T> T getInstance(
			final Enum<?> property,
			final Class<T> iface,
			final Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException {
		try {
			final String propName = GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property);
			if (jobConfiguration.getRaw(propName) == null) {
				if (defaultValue == null) {
					return null;
				}
				logger.warn("Using default for property " + propName);
			}
			return jobConfiguration.getClass(
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

	public double getDouble(
			final Enum<?> property,
			final double defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (jobConfiguration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return jobConfiguration.getDouble(
				propName,
				defaultValue);
	}

	public byte[] getBytes(
			final Enum<?> property ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		final String data = jobConfiguration.getRaw(propName);
		if (data == null) {
			logger.error(propName + " not found ");
		}
		return ByteArrayUtils.byteArrayFromString(data);
	}

}