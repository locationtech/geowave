package mil.nga.giat.geowave.test.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppClassLoader;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.test.AccumuloStoreTestEnvironment;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestEnvironment;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;

public class ServicesTestEnvironment implements
		TestEnvironment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicesTestEnvironment.class);

	private static ServicesTestEnvironment singletonInstance = null;

	public static synchronized ServicesTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new ServicesTestEnvironment();
		}
		return singletonInstance;
	}

	protected static final int JETTY_PORT = 9011;
	protected static final String JETTY_BASE_URL = "http://localhost:" + JETTY_PORT;
	protected static final int ACCEPT_QUEUE_SIZE = 100;
	protected static final int MAX_IDLE_TIME = (int) TimeUnit.HOURS.toMillis(1);
	protected static final int SO_LINGER_TIME = -1;
	protected static final int MAX_FORM_CONTENT_SIZE = 1024 * 1024 * 2;
	protected static final String GEOSERVER_USER = "admin";
	protected static final String GEOSERVER_PASS = "geoserver";
	protected static final String TEST_WORKSPACE = "geowave_test";
	protected static final String GEOSERVER_WAR_DIR = "target/geoserver";
	protected static final String GEOSERVER_CONTEXT_PATH = "/geoserver";
	protected static final String GEOSERVER_BASE_URL = JETTY_BASE_URL + GEOSERVER_CONTEXT_PATH;
	protected static final String GEOSERVER_REST_PATH = GEOSERVER_BASE_URL + "/rest";
	protected static final String GEOWAVE_WAR_DIR = "target/geowave-services";
	protected static final String GEOWAVE_CONTEXT_PATH = "/geowave-services";
	protected static final String GEOWAVE_BASE_URL = JETTY_BASE_URL + GEOWAVE_CONTEXT_PATH;
	protected static final String GEOWAVE_WORKSPACE_PATH = GEOSERVER_WAR_DIR + "/data/workspaces/" + TEST_WORKSPACE;
	protected static final String TEST_STYLE_NAME_NO_DIFFERENCE = "DecimatePoints-2px";
	protected static final String TEST_STYLE_NAME_MINOR_SUBSAMPLE = "DecimatePoints-10px";
	protected static final String TEST_STYLE_NAME_MAJOR_SUBSAMPLE = "DecimatePoints-100px";
	protected static final String TEST_STYLE_PATH = "src/test/resources/sld/";
	protected static final String TEST_SLD_NO_DIFFERENCE_FILE = TEST_STYLE_PATH + TEST_STYLE_NAME_NO_DIFFERENCE
			+ ".sld";
	protected static final String TEST_SLD_MINOR_SUBSAMPLE_FILE = TEST_STYLE_PATH + TEST_STYLE_NAME_MINOR_SUBSAMPLE
			+ ".sld";
	protected static final String TEST_SLD_MAJOR_SUBSAMPLE_FILE = TEST_STYLE_PATH + TEST_STYLE_NAME_MAJOR_SUBSAMPLE
			+ ".sld";

	private Server jettyServer;

	@SuppressFBWarnings(value = {
		"SWL_SLEEP_WITH_LOCK_HELD"
	}, justification = "Jetty must be started before releasing the lock")
	@Override
	public void setup()
			throws Exception {
		synchronized (GeoWaveITRunner.MUTEX) {
			// Setup activities delegated to private function
			// to satisfy HP Fortify
			doSetup();
		}
	}

	private void doSetup() {
		if (jettyServer == null) {
			try {
				// Prevent "Unauthorized class found" error
				System.setProperty(
						"GEOSERVER_XSTREAM_WHITELIST",
						"org.geoserver.wfs.**;org.geoserver.wms.**");

				// delete old workspace configuration if it's still there
				jettyServer = new Server();

				final SocketConnector conn = new SocketConnector();
				conn.setPort(JETTY_PORT);
				conn.setAcceptQueueSize(ACCEPT_QUEUE_SIZE);
				conn.setMaxIdleTime(MAX_IDLE_TIME);
				conn.setSoLingerTime(SO_LINGER_TIME);
				jettyServer.setConnectors(new Connector[] {
					conn
				});

				final WebAppContext gsWebapp = new WebAppContext();
				gsWebapp.setContextPath(GEOSERVER_CONTEXT_PATH);
				gsWebapp.setWar(GEOSERVER_WAR_DIR);

				final WebAppClassLoader classLoader = AccessController
						.doPrivileged(new PrivilegedAction<WebAppClassLoader>() {
							@Override
							public WebAppClassLoader run() {
								try {
									return new WebAppClassLoader(
											gsWebapp);
								}
								catch (final IOException e) {
									LOGGER.error(
											"Unable to create new classloader",
											e);
									return null;
								}
							}
						});
				if (classLoader == null) {
					throw new IOException(
							"Unable to create classloader");
				}

				// new WebAppClassLoader(
				// gsWebapp);
				classLoader.addClassPath(System.getProperty(
						"java.class.path").replace(
						":",
						";"));
				gsWebapp.setClassLoader(classLoader);
				gsWebapp.setParentLoaderPriority(true);

				final File warDir = new File(
						GEOWAVE_WAR_DIR);

				// update the config file
				ServicesTestUtils.writeConfigFile(new File(
						warDir,
						"/WEB-INF/config.properties"));

				final WebAppContext gwWebapp = new WebAppContext();
				gwWebapp.setContextPath(GEOWAVE_CONTEXT_PATH);
				gwWebapp.setWar(warDir.getAbsolutePath());

				jettyServer.setHandlers(new WebAppContext[] {
					gsWebapp,
					gwWebapp
				});
				gsWebapp.setTempDirectory(TestUtils.TEMP_DIR);
				// this allows to send large SLD's from the styles form
				gsWebapp.getServletContext().getContextHandler().setMaxFormContentSize(
						MAX_FORM_CONTENT_SIZE);

				final String jettyConfigFile = System.getProperty("jetty.config.file");
				if (jettyConfigFile != null) {
					LOGGER.info("Loading Jetty config from file: " + jettyConfigFile);
					(new XmlConfiguration(
							new FileInputStream(
									jettyConfigFile))).configure(jettyServer);
				}

				jettyServer.start();
				while (!jettyServer.isRunning() && !jettyServer.isStarted()) {
					Thread.sleep(1000);
				}

				// use this to test normal stop behavior, that is, to check
				// stuff that need to be done on container shutdown (and
				// yes, this will make jetty stop just after you started
				// it...)

				// jettyServer.stop();

			}
			catch (final RuntimeException e) {
				throw e;
			}
			catch (final Exception e) {
				LOGGER.error(
						"Could not start the Jetty server: " + e.getMessage(),
						e);

				if (jettyServer != null) {
					try {
						jettyServer.stop();
					}
					catch (final Exception e1) {
						LOGGER.error(
								"Unable to stop the Jetty server",
								e1);
					}
				}
			}
		}
	}

	@Override
	public void tearDown()
			throws Exception {
		synchronized (GeoWaveITRunner.MUTEX) {
			if (!GeoWaveITRunner.DEFER_CLEANUP.get()) {
				if (jettyServer != null) {
					try {
						jettyServer.stop();
						jettyServer = null;
					}
					catch (final Exception e) {
						LOGGER.error(
								"Unable to stop the Jetty server",
								e);
					}
				}
			}
		}
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {
			MapReduceTestEnvironment.getInstance()
		};
	}
}
