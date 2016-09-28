package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.state.SetGoalState;
// @formatter:off
/*if[accumulo.api=1.6]
import org.apache.accumulo.minicluster.ServerType;
else[accumulo.api=1.6]*/
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterControl;
/*end[accumulo.api=1.6]*/
// @formatter:on
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.minicluster.GeoWaveMiniAccumuloClusterImpl;
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
			"./target/accumulo_temp"); // breaks
										// on
										// windows
										// if
										// temp
										// directory
										// isn't
										// on
										// same
										// drive
										// as
										// project
	protected String zookeeper;
	protected String accumuloInstance;
	protected String accumuloUser;
	protected String accumuloPassword;
	protected MiniAccumuloClusterImpl miniAccumulo;

	private ExecutorService executor;
	private List<Process> cleanup = new ArrayList<Process>();

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
					config.setZooKeeperPort(Integer.parseInt(zookeeper.split(":")[1]));
					config.setNumTservers(2);

					miniAccumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
							config,
							AccumuloStoreTestEnvironment.class);

					startMiniAccumulo(config);
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

	private void startMiniAccumulo(
			MiniAccumuloConfigImpl config )
			throws IOException,
			InterruptedException {

		Runtime.getRuntime().addShutdownHook(
				new Thread() {
					@Override
					public void run() {
						tearDown();
					}
				});

		// @formatter:off
		/*if[accumulo.api=1.6]
		Process initProcess = miniAccumulo.exec(Initialize.class, "--instance-name", config.getInstanceName(), "--password", config.getRootPassword());
		else[accumulo.api=1.6]*/
		MiniAccumuloClusterControl control = miniAccumulo.getClusterControl();

		Map<String, String> siteConfig = config.getSiteConfig();
		siteConfig.put(
				Property.INSTANCE_ZK_HOST.getKey(),
				zookeeper);
		config.setSiteConfig(siteConfig);

		LinkedList<String> args = new LinkedList<>();
		args.add("--instance-name");
		args.add(config.getInstanceName());
		args.add("--user");
		args.add(config.getRootUserName());

		// If we aren't using SASL, add in the root password
		final String saslEnabled = config.getSiteConfig().get(
				Property.INSTANCE_RPC_SASL_ENABLED.getKey());
		if (null == saslEnabled || !Boolean.parseBoolean(saslEnabled)) {
			args.add("--password");
			args.add(config.getRootPassword());
		}
		
		Process initProcess = miniAccumulo.exec(
				Initialize.class,
				args.toArray(new String[0]));
		/*end[accumulo.api=1.6]*/
		// @formatter:on

		cleanup.add(initProcess);

		int ret = initProcess.waitFor();
		if (ret != 0) {
			throw new RuntimeException(
					"Initialize process returned " + ret + ". Check the logs in " + config.getLogDir() + " for errors.");
		}

		LOGGER.info("Starting MAC against instance " + config.getInstanceName() + " and zookeeper(s)  "
				+ config.getZooKeepers());

		// @formatter:off
		/*if[accumulo.api=1.6]
		for (int i = 0; i < config.getNumTservers(); i++) {
			// Note that in accumulo 1.6 this will result in default memory options 
			// (rather than specific tablet server memory options) being applied
			cleanup.add(miniAccumulo.exec(TabletServer.class));
	    }
		else[accumulo.api=1.6]*/
		control.start(ServerType.TABLET_SERVER);
		/*end[accumulo.api=1.6]*/
		// @formatter:on

		ret = 0;
		for (int i = 0; i < 5; i++) {
			Process proc = miniAccumulo.exec(
					Main.class,
					SetGoalState.class.getName(),
					MasterGoalState.NORMAL.toString());
			cleanup.add(proc);
			ret = proc.waitFor();
			if (ret == 0) break;
			UtilWaitThread.sleep(1000);
		}
		if (ret != 0) {
			throw new RuntimeException(
					"Could not set master goal state, process returned " + ret + ". Check the logs in "
							+ config.getLogDir() + " for errors.");
		}

		// @formatter:off
		/*if[accumulo.api=1.6]
		// Note that in accumulo 1.6 this will result in default memory options 
		// (rather than specific ServerType-specific memory options) being applied
		cleanup.add(miniAccumulo.exec(Master.class));
		cleanup.add(miniAccumulo.exec(SimpleGarbageCollector.class));
		else[accumulo.api=1.6]*/	    
		control.start(ServerType.MASTER);
		control.start(ServerType.GARBAGE_COLLECTOR);

		executor = Executors.newSingleThreadExecutor();

		((GeoWaveMiniAccumuloClusterImpl) miniAccumulo).setExternalShutdownExecutor(executor);
		/*end[accumulo.api=1.6]*/
		// @formatter:on
	}

	@Override
	public void tearDown() {
		zookeeper = null;
		accumuloInstance = null;
		accumuloUser = null;
		accumuloPassword = null;
		if (miniAccumulo != null) {
			try {

				// @formatter:off
				/*if[accumulo.api=1.6]
				for (Process p : cleanup) {
					p.destroy();
					p.waitFor();
				}
				else[accumulo.api=1.6]*/
				MiniAccumuloClusterControl control = miniAccumulo.getClusterControl();

				try {
					control.stop(
							ServerType.GARBAGE_COLLECTOR,
							null);
					control.stop(
							ServerType.MASTER,
							null);
					control.stop(
							ServerType.TABLET_SERVER,
							null);
				} catch (final IOException e) {
					LOGGER.warn(
							"Unable to stop mini accumulo instance",
							e);
				}

				if (executor != null) {
					executor.shutdownNow();
				}

				for (Process p : cleanup) {
					p.destroy();
					p.waitFor();
				}
				/*end[accumulo.api=1.6]*/
				// @formatter:on

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
		return new TestEnvironment[] {
			ZookeeperTestEnvironment.getInstance()
		};
	}
}
