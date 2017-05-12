package mil.nga.giat.geowave.test.config;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.config.SetCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreFactory;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
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

public class ConfigCacheIT
{

	public File configFile = null;
	public ManualOperationParams operationParams = null;

	private final static Logger LOGGER = LoggerFactory.getLogger(ConfigCacheIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING ConfigCacheIT         *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED ConfigCacheIT           *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

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
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
	}

	@After
	public void after() {
		if (configFile.exists()) {
			configFile.delete();
		}
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().remove(
				"memory");
	}

	@Test
	public void addStore() {
		final String storeName = new MemoryDataStoreFactory().getType();

		final AddStoreCommand command = new AddStoreCommand();
		command.setParameters("abc");
		command.setMakeDefault(true);
		command.setStoreType(storeName);

		// This will load the params via SPI.
		command.prepare(operationParams);

		final DataStorePluginOptions options = command.getPluginOptions();

		final MemoryRequiredOptions opts = (MemoryRequiredOptions) options.getFactoryOptions();
		opts.setGeowaveNamespace("namespace");

		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				"namespace",
				props.getProperty("store.abc.opts." + StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION));
		Assert.assertEquals(
				"abc",
				props.getProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE));
	}

	@Test
	public void addStoreFromDefault() {
		addStore();

		// Now make from default
		final AddStoreCommand command = new AddStoreCommand();
		command.setParameters("abc2");
		command.setMakeDefault(false);

		// This will load the params via SPI.
		command.prepare(operationParams);

		final DataStorePluginOptions options = command.getPluginOptions();

		final MemoryRequiredOptions opts = (MemoryRequiredOptions) options.getFactoryOptions();
		opts.setGeowaveNamespace("namespace2");

		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				"namespace2",
				props.getProperty("store.abc2.opts." + StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION));
	}

	@Test
	public void copyStore() {
		addStore();

		// Now make from default
		final CopyStoreCommand command = new CopyStoreCommand();
		command.setParameters(
				"abc",
				"abc2");

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				"namespace",
				props.getProperty("store.abc2.opts." + StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION));
	}

	@Test
	public void removeStore() {
		addStore();

		final RemoveStoreCommand command = new RemoveStoreCommand();
		command.setEntryName("abc");

		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				1,
				props.size());
	}

	@Test
	public void addIndex() {
		final String spatialType = new SpatialDimensionalityTypeProvider().getDimensionalityTypeName();

		final AddIndexCommand command = new AddIndexCommand();
		command.setParameters("abc");
		command.setMakeDefault(true);
		command.setType(spatialType);

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
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

		final String spatialType = new SpatialDimensionalityTypeProvider().getDimensionalityTypeName();

		final AddIndexCommand command = new AddIndexCommand();
		command.setParameters("abc2");
		command.setMakeDefault(false);

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				spatialType,
				props.getProperty("index.abc2.type"));
	}

	@Test
	public void copyIndex() {
		addIndex();

		final String spatialType = new SpatialDimensionalityTypeProvider().getDimensionalityTypeName();

		final CopyIndexCommand command = new CopyIndexCommand();
		command.setParameters(
				"abc",
				"abc2");

		// This will load the params via SPI.
		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				spatialType,
				props.getProperty("index.abc2.type"));
	}

	@Test
	public void removeIndex() {
		addIndex();

		final RemoveIndexCommand command = new RemoveIndexCommand();
		command.setEntryName("abc");

		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		Assert.assertEquals(
				1,
				props.size());
	}

	@Test
	public void addIndexGroup() {
		addIndex();

		final AddIndexGroupCommand command = new AddIndexGroupCommand();
		command.setParameters(
				"ig1",
				"abc");

		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
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
		final RemoveIndexCommand commandRemove = new RemoveIndexCommand();
		commandRemove.setEntryName("abc");
		commandRemove.prepare(operationParams);
		commandRemove.execute(operationParams);
		// ABOVE: Just to remove the index

		final RemoveIndexGroupCommand command = new RemoveIndexGroupCommand();
		command.setEntryName("ig1");

		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
				configFile,
				null);
		Assert.assertEquals(
				1,
				props.size());
	}

	@Test
	public void set() {
		final SetCommand command = new SetCommand();
		command.setParameters(
				"lala",
				"5");
		command.prepare(operationParams);
		command.execute(operationParams);

		final Properties props = ConfigOptions.loadProperties(
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
