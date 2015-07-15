package mil.nga.giat.geowave.analytic.log;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;

import org.slf4j.Logger;

public class LoggingConfigurationWrapper implements
		ConfigurationWrapper
{
	final private Logger logger;
	final private ConfigurationWrapper wrapper;

	public LoggingConfigurationWrapper(
			final Logger logger,
			final ConfigurationWrapper wrapper ) {
		super();
		this.logger = logger;
		this.wrapper = wrapper;
	}

	@Override
	public int getInt(
			final Enum<?> property,
			final Class<?> scope,
			final int defaultValue ) {

		final int v = wrapper.getInt(
				property,
				scope,
				defaultValue);
		logger.info(
				"Requesting {}. Returning {}",
				property.name(),
				v);

		return v;

	}

	@Override
	public double getDouble(
			final Enum<?> property,
			final Class<?> scope,
			final double defaultValue ) {
		final double v = wrapper.getDouble(
				property,
				scope,
				defaultValue);
		logger.info(
				"Requesting {}. Returning {}",
				property.name(),
				v);

		return v;
	}

	@Override
	public String getString(
			final Enum<?> property,
			final Class<?> scope,
			final String defaultValue ) {
		final String v = wrapper.getString(
				property,
				scope,
				defaultValue);
		logger.info(
				"Requesting {}. Returning {}",
				property.name(),
				v);

		return v;
	}

	@Override
	public byte[] getBytes(
			final Enum<?> property,
			final Class<?> scope ) {
		final byte[] v = wrapper.getBytes(
				property,
				scope);
		logger.info(
				"Requesting {}. Returning {}",
				property.name(),
				v);

		return v;
	}

	@Override
	public <T> T getInstance(
			final Enum<?> property,
			final Class<?> scope,
			final Class<T> iface,
			final Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException {
		final T v = wrapper.getInstance(
				property,
				scope,
				iface,
				defaultValue);
		logger.info(
				"Requesting {}. Returning {}",
				property.name(),
				v.getClass().getName());

		return v;
	}
}
