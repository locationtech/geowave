package mil.nga.giat.geowave.core.cli.spi;

import java.util.Properties;

public interface DefaultConfigProviderSpi
{
	/**
	 * Returns the default configurations form the project
	 * 
	 * @return default configuration
	 */
	public Properties getDefaultConfig();
}
