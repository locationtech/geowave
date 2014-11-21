package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TimeZone;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.google.common.io.Files;

@RunWith(Suite.class)
@SuiteClasses({
	//GeoWaveBasicIT.class,
	//GeoWaveMapReduceIT.class,
	GeoServerIT.class
})
public class GeoWaveITSuite
{
	protected static final String TEST_FILTER_START_TIME_ATTRIBUTE_NAME = "StartTime";
	protected static final String TEST_FILTER_END_TIME_ATTRIBUTE_NAME = "EndTime";
	protected static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test";
	protected static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	protected static final String TEST_CASE_BASE = "data/";
	protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";
	private final static Logger LOGGER = Logger.getLogger(GeoWaveITSuite.class);
	protected static AccumuloOperations accumuloOperations;
	protected static String zookeeper;
	protected static String accumuloInstance;
	protected static String accumuloUser;
	protected static String accumuloPassword;
	protected static MiniAccumuloCluster miniAccumulo;
	protected static File tempDir;

	protected static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

	@BeforeClass
	public static void setup() {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		zookeeper = System.getProperty("zookeeperUrl");
		accumuloInstance = System.getProperty("instance");
		accumuloUser = System.getProperty("username");
		accumuloPassword = System.getProperty("password");
		if (!isSet(zookeeper) || !isSet(accumuloInstance) || !isSet(accumuloUser) || !isSet(accumuloPassword)) {
			try {
				tempDir = Files.createTempDir();
				tempDir.deleteOnExit();
				final MiniAccumuloConfig config = new MiniAccumuloConfig(
						tempDir,
						DEFAULT_MINI_ACCUMULO_PASSWORD);
				config.setNumTservers(2);
				miniAccumulo = new MiniAccumuloCluster(
						config);
				if (SystemUtils.IS_OS_WINDOWS && isYarn()) {
					// this must happen after instantiating Mini Accumulo
					// Cluster because it ensures the accumulo directory is
					// empty or it will fail, but must happen before the cluster
					// is started because yarn expects winutils.exe to exist
					// within a bin directory in the mini accumulo cluster
					// directory (mini accumulo cluster will always set this
					// directory as hadoop_home)
					LOGGER.info("Running YARN on windows requires a local installation of Hadoop");
					LOGGER.info("'HADOOP_HOME' must be set and 'PATH' must contain %HADOOP_HOME%/bin");

					final Map<String, String> env = System.getenv();
					String hadoopHome = System.getProperty("hadoop.home.dir");
					if (hadoopHome == null) {
						hadoopHome = env.get("HADOOP_HOME");
					}
					boolean success = false;
					if (hadoopHome != null) {
						final File hadoopDir = new File(
								hadoopHome);
						if (hadoopDir.exists()) {
							final File binDir = new File(
									tempDir,
									"bin");
							binDir.mkdir();
							FileUtils.copyFile(
									new File(
											hadoopDir + File.separator + "bin",
											HADOOP_WINDOWS_UTIL),
									new File(
											binDir,
											HADOOP_WINDOWS_UTIL));
							success = true;
						}
					}
					if (!success) {
						LOGGER.error("'HADOOP_HOME' environment variable is not set or <HADOOP_HOME>/bin/winutils.exe does not exist");
						return;
					}
				}

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
				LOGGER.info("Check '" + tempDir.getAbsolutePath() + File.separator + "logs' for more info");
				if (SystemUtils.IS_OS_WINDOWS) {
					LOGGER.warn("For windows, make sure that Cygwin is installed and set a CYGPATH environment variable to %CYGWIN_HOME%/bin/cygpath to successfully run a mini accumulo cluster");
				}
				Assert.fail("Unable to start mini accumulo instance: '" + e.getLocalizedMessage() + "'");
			}
		}
		try {
			accumuloOperations = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					TEST_NAMESPACE);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to connect to Accumulo",
					e);
			Assert.fail("Could not connect to Accumulo instance: '" + e.getLocalizedMessage() + "'");
		}
	}

	protected static boolean isSet(
			final String str ) {
		return (str != null) && !str.isEmpty();
	}

	@AfterClass
	public static void cleanup() {
		Assert.assertTrue(
				"Index not deleted successfully",
				(accumuloOperations == null) || accumuloOperations.deleteAll());
		accumuloOperations = null;
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
			try {
				// sleep because mini accumulo processes still have a hold on
				// the log files and there is no hook to get notified when it is
				// completely stopped
				Thread.sleep(1000);
				FileUtils.deleteDirectory(tempDir);
				tempDir = null;
			}
			catch (final IOException | InterruptedException e) {
				LOGGER.warn(
						"Unable to delete mini Accumulo temporary directory",
						e);
			}
		}
	}
}
