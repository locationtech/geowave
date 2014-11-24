package mil.nga.giat.geowave.analytics.tools;

public interface ConfigurationWrapper
{
	public int getInt(
			Enum<?> property,
			Class<?> scope,
			int defaultValue );

	public String getString(
			Enum<?> property,
			Class<?> scope,
			String defaultValue );

	public <T> T getInstance(
			Enum<?> property,
			Class<?> scope,
			Class<T> iface,
			Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException;
}
