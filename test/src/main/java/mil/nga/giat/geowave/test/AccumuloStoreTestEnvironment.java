package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class AccumuloStoreTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new AccumuloDataStoreFactory();
	private static AccumuloStoreTestEnvironment singletonInstance = null;

	public static synchronized AccumuloStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new AccumuloStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloStoreTestEnvironment.class);
	protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";
	protected static final String HADOOP_DLL = "hadoop.dll";
	// breaks on windows if temp directory isn't on same drive as project
	protected static final File TEMP_DIR = new File(
			"./target/accumulo_temp");
	protected String zookeeper;
	protected String accumuloInstance;
	protected String accumuloUser;
	protected String accumuloPassword;
	protected MiniAccumuloClusterImpl miniAccumulo;

	private final List<Process> cleanup = new ArrayList<Process>();

	private AccumuloStoreTestEnvironment() {}

	@Override
	public void setup() {

		if (!TestUtils.isSet(zookeeper)) {
			zookeeper = System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME);

			if (!TestUtils.isSet(zookeeper)) {
				zookeeper = ZookeeperTestEnvironment.getInstance().getZookeeper();
				LOGGER.debug("Using local zookeeper URL: " + zookeeper);
			}
		}

		if (!TestUtils.isSet(accumuloInstance) || !TestUtils.isSet(accumuloUser) || !TestUtils.isSet(accumuloPassword)) {

			accumuloInstance = System.getProperty("instance");
			accumuloUser = System.getProperty("username");
			accumuloPassword = System.getProperty("password");
			if (!TestUtils.isSet(accumuloInstance) || !TestUtils.isSet(accumuloUser)
					|| !TestUtils.isSet(accumuloPassword)) {
				try {
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
					config.setZooKeeperPort(Integer.parseInt(zookeeper.split(":")[1]));
					config.setNumTservers(2);

					miniAccumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
							config,
							AccumuloStoreTestEnvironment.class);

					startMiniAccumulo(config);
					accumuloUser = "root";
					accumuloInstance = miniAccumulo.getInstanceName();
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

	private void startMiniAccumulo(
			final MiniAccumuloConfigImpl config )
			throws IOException,
			InterruptedException {

		final LinkedList<String> jvmArgs = new LinkedList<>();
		jvmArgs.add("-XX:CompressedClassSpaceSize=512m");
		jvmArgs.add("-XX:MaxMetaspaceSize=512m");
		jvmArgs.add("-Xmx512m");

		Runtime.getRuntime().addShutdownHook(
				new Thread() {
					@Override
					public void run() {
						tearDown();
					}
				});
		final Map<String, String> siteConfig = config.getSiteConfig();
		siteConfig.put(
				Property.INSTANCE_ZK_HOST.getKey(),
				zookeeper);
		config.setSiteConfig(siteConfig);

		final LinkedList<String> args = new LinkedList<>();
		args.add("--instance-name");
		args.add(config.getInstanceName());
		args.add("--password");
		args.add(config.getRootPassword());

		final Process initProcess = miniAccumulo.exec(
				Initialize.class,
				jvmArgs,
				args.toArray(new String[0]));

		cleanup.add(initProcess);

		final int ret = initProcess.waitFor();
		if (ret != 0) {
			throw new RuntimeException(
					"Initialize process returned " + ret + ". Check the logs in " + config.getLogDir() + " for errors.");
		}

		LOGGER.info("Starting MAC against instance " + config.getInstanceName() + " and zookeeper(s)  "
				+ config.getZooKeepers());

		for (int i = 0; i < config.getNumTservers(); i++) {
			cleanup.add(miniAccumulo.exec(
					TabletServer.class,
					jvmArgs));
		}

		cleanup.add(miniAccumulo.exec(
				Master.class,
				jvmArgs));
		cleanup.add(miniAccumulo.exec(
				SimpleGarbageCollector.class,
				jvmArgs));
	}

	@Override
	public void tearDown() {
		zookeeper = null;
		accumuloInstance = null;
		accumuloUser = null;
		accumuloPassword = null;
		if (miniAccumulo != null) {
			try {

				for (final Process p : cleanup) {
					p.destroy();
					p.waitFor();
				}

				for (final Process p : cleanup) {
					p.destroy();
					p.waitFor();
				}

				miniAccumulo = null;

			}
			catch (final InterruptedException e) {
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
	protected void initOptions(
			final StoreFactoryOptions options ) {
		final AccumuloRequiredOptions accumuloOpts = (AccumuloRequiredOptions) options;
		accumuloOpts.setUser(accumuloUser);
		accumuloOpts.setPassword(accumuloPassword);
		accumuloOpts.setInstance(accumuloInstance);
		accumuloOpts.setZookeeper(zookeeper);
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.ACCUMULO;
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
		return new TestEnvironment[] {
			ZookeeperTestEnvironment.getInstance()
		};
	}
}
