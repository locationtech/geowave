package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;

import com.google.common.io.Files;

public class GeowaveTestEnvironment
{
	protected final static Logger LOGGER = Logger.getLogger(GeowaveTestEnvironment.class);
	protected static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test_GeoWaveIT";

	protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	protected static AccumuloOperations accumuloOperations;
	protected static String zookeeper;
	protected static String accumuloInstance;
	protected static String accumuloUser;
	protected static String accumuloPassword;
	protected static MiniAccumuloCluster miniAccumulo;
	protected static File tempDir;

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
				config.setNumTservers(4);
				miniAccumulo = new MiniAccumuloCluster(
						config);
				miniAccumulo.start();
				zookeeper = miniAccumulo.getZooKeepers();
				accumuloInstance = miniAccumulo.getInstanceName();
				accumuloUser = "root";
				accumuloPassword = DEFAULT_MINI_ACCUMULO_PASSWORD;
			}
			catch (IOException  | InterruptedException e) {
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
	
	public Credentials getCredentials() {
	  return new UsernamePasswordCredentials("admin","geoserver"); //"root","L.}GiBeC"); 	
	}
	
	public static void addAuthorization(String auth) {
		try {
			((BasicAccumuloOperations)accumuloOperations).insureAuthorization(auth);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to alter authorization for Accumulo user",
					e);
			Assert.fail("Unable to alter authorization for Accumulo user: '" + e.getLocalizedMessage() + "'");
		}
	}

	@AfterClass
	public static void cleanup() {
		Assert.assertTrue(
				"Index not deleted successfully",
				accumuloOperations == null || accumuloOperations.deleteAll());
		if (miniAccumulo != null) {
			try {
				miniAccumulo.stop();
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
			}
			catch (final IOException | InterruptedException e) {
				LOGGER.warn(
						"Unable to delete mini Accumulo temporary directory",
						e);
			}
		}
	}

	private static boolean isSet(
			final String str ) {
		return (str != null) && !str.isEmpty();
	}

	
	/**
	 * Unzips the contents of a zip input stream to a target output directory if
	 * the file exists and is the same size as the zip entry, it is not
	 * overwritten
	 * 
	 * @param zipFile
	 *            input zip file
	 * @param output
	 *            zip file output folder
	 */
	protected static void unZipFile(
			final InputStream zipStream,
			final String outputFolder ) {

		final byte[] buffer = new byte[1024];

		try {

			// create output directory is not exists
			final File folder = new File(
					outputFolder);
			if (!folder.exists()) {
				folder.mkdir();
			}

			// get the zip file content
			final ZipInputStream zis = new ZipInputStream(
					zipStream);
			// get the zipped file list entry
			ZipEntry ze = zis.getNextEntry();

			while (ze != null) {
				if (ze.isDirectory()) {
					ze = zis.getNextEntry();
					continue;
				}
				final String fileName = ze.getName();
				final File newFile = new File(
						outputFolder + File.separator + fileName);
				if (newFile.exists()) {
					if (newFile.length() == ze.getSize()) {
						ze = zis.getNextEntry();
						continue;
					}
					else {
						newFile.delete();
					}
				}

				// create all non exists folders
				new File(
						newFile.getParent()).mkdirs();

				final FileOutputStream fos = new FileOutputStream(
						newFile);

				int len;
				while ((len = zis.read(buffer)) > 0) {
					fos.write(
							buffer,
							0,
							len);
				}

				fos.close();
				ze = zis.getNextEntry();
			}

			zis.closeEntry();
			zis.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to extract test data",
					e);
			Assert.fail("Unable to extract test data: '" + e.getLocalizedMessage() + "'");
		}
	}
	
	static protected void replaceParameters(
			Map<String, String> values,
			File file ) throws IOException {
		{
			String str = FileUtils.readFileToString(file);
			for (Entry<String, String> entry : values.entrySet()) {
				str = str.replaceAll(
						entry.getKey(),
						entry.getValue());
			}
			FileUtils.deleteQuietly(file);
			FileUtils.write(
					file,
					str);
		}
	}
}
