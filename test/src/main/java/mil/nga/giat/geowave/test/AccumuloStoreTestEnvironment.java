package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

public class AccumuloStoreTestEnvironment implements
		StoreTestEnvironment
{
	private static AccumuloStoreTestEnvironment singletonInstance = null;

	public static synchronized AccumuloStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new AccumuloStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = Logger.getLogger(AccumuloStoreTestEnvironment.class);
	protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";
	protected static final String HADOOP_DLL = "hadoop.dll";
	protected static final File TEMP_DIR = new File(
			"./target/accumulo_temp"); // breaks on windows if temp directory
										// isn't on same drive as project
	protected String zookeeper;
	protected String accumuloInstance;
	protected String accumuloUser;
	protected String accumuloPassword;
	protected MiniAccumuloClusterImpl miniAccumulo;

	private AccumuloStoreTestEnvironment() {}

	@Override
	public void setup() {
		if (!TestUtils.isSet(zookeeper) || !TestUtils.isSet(accumuloInstance) || !TestUtils.isSet(accumuloUser)
				|| !TestUtils.isSet(accumuloPassword)) {
			zookeeper = System.getProperty("zookeeperUrl");
			accumuloInstance = System.getProperty("instance");
			accumuloUser = System.getProperty("username");
			accumuloPassword = System.getProperty("password");
			if (!TestUtils.isSet(zookeeper) || !TestUtils.isSet(accumuloInstance) || !TestUtils.isSet(accumuloUser)
					|| !TestUtils.isSet(accumuloPassword)) {
				try {

					// TEMP_DIR = Files.createTempDir();
					if (!TEMP_DIR.exists()) {
						if (!TEMP_DIR.mkdirs()) {
							throw new IOException(
									"Could not create temporary directory");
						}
					}
					TEMP_DIR.deleteOnExit();
					final MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(
							TEMP_DIR,
							DEFAULT_MINI_ACCUMULO_PASSWORD);
					config.setNumTservers(2);

					miniAccumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
							config,
							AccumuloStoreTestEnvironment.class);

					miniAccumulo.start();
					zookeeper = miniAccumulo.getZooKeepers();
					accumuloInstance = miniAccumulo.getInstanceName();
					accumuloUser = "root";
					accumuloPassword = DEFAULT_MINI_ACCUMULO_PASSWORD;
				}
				catch (IOException | InterruptedException e) {
					LOGGER.warn(
							"Unable to start mini accumulo instance",
							e);
					LOGGER.info("Check '" + TEMP_DIR.getAbsolutePath() + File.separator + "logs' for more info");
					if (SystemUtils.IS_OS_WINDOWS) {
						LOGGER
								.warn("For windows, make sure that Cygwin is installed and set a CYGPATH environment variable to %CYGWIN_HOME%/bin/cygpath to successfully run a mini accumulo cluster");
					}
					Assert.fail("Unable to start mini accumulo instance: '" + e.getLocalizedMessage() + "'");
				}
			}

		}
	}

	@Override
	public void tearDown() {
		zookeeper = null;
		accumuloInstance = null;
		accumuloUser = null;
		accumuloPassword = null;
		if (miniAccumulo != null) {
			try {
				miniAccumulo.stop();
				miniAccumulo = null;
			}
			catch (IOException | InterruptedException e) {
				LOGGER.warn(
						"Unable to stop mini accumulo instance",
						e);
			}
		}
		if (TEMP_DIR != null) {
			try {
				// sleep because mini accumulo processes still have a
				// hold on the log files and there is no hook to get
				// notified when it is completely stopped

				Thread.sleep(2000);
				FileUtils.deleteDirectory(TEMP_DIR);
			}
			catch (final IOException | InterruptedException e) {
				LOGGER.warn(
						"Unable to delete mini Accumulo temporary directory",
						e);
			}
		}
	}

	@Override
	public DataStorePluginOptions getDataStoreOptions(
			final String namespace ) {
		final DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		final AccumuloRequiredOptions opts = new AccumuloRequiredOptions();
		opts.setGeowaveNamespace(namespace);
		opts.setUser(accumuloUser);
		opts.setPassword(accumuloPassword);
		opts.setInstance(accumuloInstance);
		opts.setZookeeper(zookeeper);
		pluginOptions.selectPlugin(new AccumuloDataStoreFactory().getName());
		pluginOptions.setFactoryOptions(opts);
		return pluginOptions;
	}

	public String getZookeeper() {
		return zookeeper;
	}

	public String getAccumuloInstance() {
		return accumuloInstance;
	}

	public String getAccumuloUser() {
		return accumuloUser;
	}

	public String getAccumuloPassword() {
		return accumuloPassword;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		// TODO: create zookeeper test environment as dependency
		return new TestEnvironment[] {};
	}
}
