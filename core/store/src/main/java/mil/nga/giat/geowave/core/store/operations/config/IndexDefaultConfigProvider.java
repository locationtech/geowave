package mil.nga.giat.geowave.core.store.operations.config;

import java.util.Properties;

import mil.nga.giat.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class IndexDefaultConfigProvider implements
		DefaultConfigProviderSpi
{
	private Properties configProperties = new Properties();

	/**
	 * Create the properties for the config-properties file
	 */
	private void setProperties() {
		// Spatial Index
		configProperties.setProperty(
				"index.default-spatial.opts.numPartitions",
				"8");
		configProperties.setProperty(
				"index.default-spatial.opts.partitionStrategy",
				"HASH");
		configProperties.setProperty(
				"index.default-spatial.opts.storeTime",
				"false");
		configProperties.setProperty(
				"index.default-spatial.type",
				"spatial");
		// Spatial_Temporal Index
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.bias",
				"BALANCED");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.maxDuplicates",
				"-1");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.numPartitions",
				"8");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.partitionStrategy",
				"HASH");
		configProperties.setProperty(
				"index.default-spatial-temporal.opts.period",
				"YEAR");
		configProperties.setProperty(
				"index.default-spatial-temporal.type",
				"spatial_temporal");
	}

	@Override
	public Properties getDefaultConfig() {
		setProperties();
		return configProperties;
	}

}
