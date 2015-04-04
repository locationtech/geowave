package mil.nga.giat.geowave.analytics.tools;

public interface ConfigurationWrapper
{
	public int getInt(
			Enum<?> property,
			Class<?> scope,
			int defaultValue );

	public double getDouble(
			Enum<?> property,
			Class<?> scope,
			double defaultValue );

	public String getString(
			Enum<?> property,
			Class<?> scope,
			String defaultValue );

	public byte[] getBytes(
			Enum<?> property,
			Class<?> scope );

	public <T> T getInstance(
			Enum<?> property,
			Class<?> scope,
			Class<T> iface,
			Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException;
}
