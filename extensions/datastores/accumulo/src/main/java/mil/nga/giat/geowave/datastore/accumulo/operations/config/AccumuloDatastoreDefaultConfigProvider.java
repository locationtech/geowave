package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import java.util.Properties;

import mil.nga.giat.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class AccumuloDatastoreDefaultConfigProvider implements
		DefaultConfigProviderSpi
{
	private Properties configProperties = new Properties();

	/**
	 * Create the properties for the config-properties file
	 */
	private void setProperties() {
		configProperties.setProperty(
				"store.default-accumulo.opts.createTable",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.enableBlockCache",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.gwNamespace",
				"geowave.default");
		configProperties.setProperty(
				"store.default-accumulo.opts.instance",
				"accumulo");
		configProperties.setProperty(
				"store.default-accumulo.opts.password",
				"secret");
		configProperties.setProperty(
				"store.default-accumulo.opts.persistAdapter",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.persistDataStatistics",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.persistIndex",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.useAltIndex",
				"false");
		configProperties.setProperty(
				"store.default-accumulo.opts.useLocalityGroups",
				"true");
		configProperties.setProperty(
				"store.default-accumulo.opts.user",
				"root");
		configProperties.setProperty(
				"store.default-accumulo.opts.zookeeper",
				"localhost:2181");
		configProperties.setProperty(
				"store.default-accumulo.type",
				"accumulo");
	}

	@Override
	public Properties getDefaultConfig() {
		setProperties();
		return configProperties;
	}

}
