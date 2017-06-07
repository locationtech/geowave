package mil.nga.giat.geowave.datastore.hbase.operations.config;

import java.util.Properties;

import mil.nga.giat.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class HBaseDatastoreDefaultConfigProvider implements
		DefaultConfigProviderSpi
{
	private Properties configProperties = new Properties();

	/**
	 * Create the properties for the config-properties file
	 */
	private void setProperties() {
		configProperties.setProperty(
				"store.default-hbase.opts.createTable",
				"true");
		configProperties.setProperty(
				"store.default-hbase.opts.disableServer",
				"false");
		configProperties.setProperty(
				"store.default-hbase.opts.disableVerifyCoprocessors",
				"false");
		configProperties.setProperty(
				"store.default-hbase.opts.enableBlockCache",
				"true");
		configProperties.setProperty(
				"store.default-hbase.opts.gwNamespace",
				"geowave.default");
		configProperties.setProperty(
				"store.default-hbase.opts.persistAdapter",
				"true");
		configProperties.setProperty(
				"store.default-hbase.opts.persistDataStatistics",
				"true");
		configProperties.setProperty(
				"store.default-hbase.opts.persistIndex",
				"true");
		configProperties.setProperty(
				"store.default-hbase.opts.scanCacheSize",
				"2147483647");
		configProperties.setProperty(
				"store.default-hbase.opts.useAltIndex",
				"false");
		configProperties.setProperty(
				"store.default-hbase.opts.zookeeper",
				"localhost:2181");
		configProperties.setProperty(
				"store.default-hbase.type",
				"hbase");
	}

	@Override
	public Properties getDefaultConfig() {
		setProperties();
		return configProperties;
	}

}
