package mil.nga.giat.geowave.test.config;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.config.SetCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.AddIndexGroupCommand;
import mil.nga.giat.geowave.core.store.operations.config.AddStoreCommand;
import mil.nga.giat.geowave.core.store.operations.config.CopyIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.CopyStoreCommand;
import mil.nga.giat.geowave.core.store.operations.config.RemoveIndexCommand;
import mil.nga.giat.geowave.core.store.operations.config.RemoveIndexGroupCommand;
import mil.nga.giat.geowave.core.store.operations.config.RemoveStoreCommand;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

public class ConfigCacheIT
{

	public File configFile = null;
	public ManualOperationParams operationParams = null;

	@Before
	public void before()
			throws IOException {
		configFile = File.createTempFile(
				"test_config",
				null);
		operationParams = new ManualOperationParams();
		operationParams.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);
	}

	@After
	public void after() {
		if (configFile.exists()) {
			configFile.delete();
		}
	}

	@Test
	public void addStore() {
		String accumuloName = new AccumuloDataStoreFactory().getName();

		AddStoreCommand command = new AddStoreCommand();
		command.setParameters("abc");
		command.setMakeDefault(true);
		command.setStoreType(accumuloName);

		// This will load the params via SPI.
		command.prepare(operationParams);

		DataStorePluginOptions options = command.getPluginOptions();

		AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options.getFactoryOptions();
		opts.setUser("user");
		opts.setPassword("pass");
		opts.setInstance("inst");
		opts.setZookeeper("zoo");

		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				"zoo",
				props.getProperty("store.abc.opts." + AccumuloRequiredOptions.ZOOKEEPER_CONFIG_KEY));
		Assert.assertEquals(
				"user",
				props.getProperty("store.abc.opts." + AccumuloRequiredOptions.USER_CONFIG_KEY));
		Assert.assertEquals(
				"inst",
				props.getProperty("store.abc.opts." + AccumuloRequiredOptions.INSTANCE_CONFIG_KEY));
		Assert.assertEquals(
				"pass",
				props.getProperty("store.abc.opts." + AccumuloRequiredOptions.PASSWORD_CONFIG_KEY));
		Assert.assertEquals(
				"abc",
				props.getProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE));
	}

	@Test
	public void addStoreFromDefault() {
		addStore();

		// Now make from default
		AddStoreCommand command = new AddStoreCommand();
		command.setParameters("abc2");
		command.setMakeDefault(false);

		// This will load the params via SPI.
		command.prepare(operationParams);

		DataStorePluginOptions options = command.getPluginOptions();

		AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options.getFactoryOptions();
		opts.setUser("user2");

		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				"zoo",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.ZOOKEEPER_CONFIG_KEY));
		Assert.assertEquals(
				"user2",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.USER_CONFIG_KEY));
		Assert.assertEquals(
				"inst",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.INSTANCE_CONFIG_KEY));
		Assert.assertEquals(
				"pass",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.PASSWORD_CONFIG_KEY));
	}

	@Test
	public void copyStore() {
		addStore();

		// Now make from default
		CopyStoreCommand command = new CopyStoreCommand();
		command.setParameters(
				"abc",
				"abc2");

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				"zoo",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.ZOOKEEPER_CONFIG_KEY));
		Assert.assertEquals(
				"user",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.USER_CONFIG_KEY));
		Assert.assertEquals(
				"inst",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.INSTANCE_CONFIG_KEY));
		Assert.assertEquals(
				"pass",
				props.getProperty("store.abc2.opts." + AccumuloRequiredOptions.PASSWORD_CONFIG_KEY));
	}

	@Test
	public void removeStore() {
		addStore();

		RemoveStoreCommand command = new RemoveStoreCommand();
		command.setEntryName("abc");

		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				1,
				props.size());
	}

	@Test
	public void addIndex() {
		String spatialType = new SpatialDimensionalityTypeProvider().getDimensionalityTypeName();

		AddIndexCommand command = new AddIndexCommand();
		command.setParameters("abc");
		command.setMakeDefault(true);
		command.setType(spatialType);

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				spatialType,
				props.getProperty("index.abc.type"));
		Assert.assertEquals(
				"abc",
				props.getProperty(IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE));
	}

	@Test
	public void addIndexFromDefault() {
		addIndex();

		String spatialType = new SpatialDimensionalityTypeProvider().getDimensionalityTypeName();

		AddIndexCommand command = new AddIndexCommand();
		command.setParameters("abc2");
		command.setMakeDefault(false);

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				spatialType,
				props.getProperty("index.abc2.type"));
	}

	@Test
	public void copyIndex() {
		addIndex();

		String spatialType = new SpatialDimensionalityTypeProvider().getDimensionalityTypeName();

		CopyIndexCommand command = new CopyIndexCommand();
		command.setParameters(
				"abc",
				"abc2");

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				spatialType,
				props.getProperty("index.abc2.type"));
	}

	@Test
	public void removeIndex() {
		addIndex();

		RemoveIndexCommand command = new RemoveIndexCommand();
		command.setEntryName("abc");

		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				1,
				props.size());
	}

	@Test
	public void addIndexGroup() {
		addIndex();

		AddIndexGroupCommand command = new AddIndexGroupCommand();
		command.setParameters(
				"ig1",
				"abc");

		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				"abc=spatial",
				props.getProperty("indexgroup.ig1.type"));
	}

	@Test
	public void removeIndexGroup() {
		addIndexGroup();

		// BELOW: Just to remove the index
		RemoveIndexCommand commandRemove = new RemoveIndexCommand();
		commandRemove.setEntryName("abc");
		commandRemove.prepare(operationParams);
		commandRemove.execute(operationParams);
		// ABOVE: Just to remove the index

		RemoveIndexGroupCommand command = new RemoveIndexGroupCommand();
		command.setEntryName("ig1");

		command.prepare(operationParams);
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);
		Assert.assertEquals(
				1,
				props.size());
	}

	@Test
	public void set() {
		SetCommand command = new SetCommand();
		command.setParameters(
				"lala",
				"5");
		command.execute(operationParams);

		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				1,
				props.size());
		Assert.assertEquals(
				"5",
				props.getProperty("lala"));
	}
}
