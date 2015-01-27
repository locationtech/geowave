package mil.nga.giat.geowave.test.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppClassLoader;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServicesTestEnvironment extends
		MapReduceTestEnvironment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicesTestEnvironment.class);

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
	protected static final String GEOSERVER_REST_PATH = JETTY_BASE_URL + GEOSERVER_CONTEXT_PATH + "/rest";
	protected static final String GEOWAVE_WAR_DIR = "target/geowave-services";
	protected static final String GEOWAVE_CONTEXT_PATH = "/geowave-services";
	protected static final String GEOWAVE_BASE_URL = JETTY_BASE_URL + GEOWAVE_CONTEXT_PATH;
	protected static Server jettyServer;

	protected static void writeConfigFile(
			final File configFile ) {
		try {
			final PrintWriter writer = new PrintWriter(
					configFile);
			writer.println("zookeeper.url=" + zookeeper);
			writer.println("zookeeper.instance=" + accumuloInstance);
			writer.println("zookeeper.username=" + accumuloUser);
			writer.println("zookeeper.password=" + accumuloPassword);
			writer.println("geoserver.url=" + JETTY_BASE_URL);
			writer.println("geoserver.username=" + GEOSERVER_USER);
			writer.println("geoserver.password=" + GEOSERVER_PASS);
			writer.println("geoserver.workspace=" + TEST_WORKSPACE);
			writer.println("hdfs=" + hdfs);
			writer.println("hdfsBase=" + hdfsBaseDirectory);
			writer.println("jobTracker=" + jobtracker);
			writer.close();
		}
		catch (final FileNotFoundException e) {
			LOGGER.error(
					"Unable to find config file",
					e);
		}
	}

	@BeforeClass
	protected static void startServices() {
		synchronized (MUTEX) {
			if (jettyServer == null) {
				try {
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

					final WebAppClassLoader classLoader = new WebAppClassLoader(
							gsWebapp);
					classLoader.addClassPath(System.getProperty(
							"java.class.path").replace(
							":",
							";"));
					gsWebapp.setClassLoader(classLoader);

					final File warDir = new File(
							GEOWAVE_WAR_DIR);

					// update the config file
					writeConfigFile(new File(
							warDir,
							"/WEB-INF/config.properties"));

					final WebAppContext gwWebapp = new WebAppContext();
					gwWebapp.setContextPath(GEOWAVE_CONTEXT_PATH);
					gwWebapp.setWar(warDir.getAbsolutePath());

					jettyServer.setHandlers(new WebAppContext[] {
						gsWebapp,
						gwWebapp
					});
					gsWebapp.setTempDirectory(tempDir);
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
	}

	@AfterClass
	public static void stopServices() {
		synchronized (MUTEX) {
			if (!DEFER_CLEANUP) {
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
}
