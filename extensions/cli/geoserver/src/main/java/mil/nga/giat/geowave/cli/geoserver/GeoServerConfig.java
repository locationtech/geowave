package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.Properties;

import org.apache.log4j.Logger;

import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.*;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.security.utils.SecurityUtils;

public class GeoServerConfig
{
	private final static Logger LOGGER = Logger.getLogger(GeoServerConfig.class);

	public static final String DEFAULT_URL = "localhost:8080";
	public static final String DEFAULT_USER = "admin";
	public static final String DEFAULT_PASS = "geoserver";
	public static final String DEFAULT_WORKSPACE = "geowave";
	public static final String DEFAULT_CS = "-raster";
	public static final String DEFAULT_DS = "-vector";

	public final static String DISPLAY_NAME_PREFIX = "GeoWave Datastore - ";
	public static final String QUERY_INDEX_STRATEGY_KEY = "Query Index Strategy";

	private String url = null;
	private String user = null;
	private String pass = null;
	private String workspace = null;

	private final File propFile;
	private final Properties gsConfigProperties;

	/**
	 * Properties File holds defaults; updates config if empty.
	 * 
	 * @param propFile
	 */
	public GeoServerConfig(
			File propFile ) {
		this.propFile = propFile;

		if (propFile != null && propFile.exists()) {
			gsConfigProperties = ConfigOptions.loadProperties(
					propFile,
					null);
		}
		else {
			gsConfigProperties = new Properties();
		}
		boolean update = false;

		url = gsConfigProperties.getProperty(GEOSERVER_URL);
		if (url == null) {
			url = DEFAULT_URL;
			gsConfigProperties.setProperty(
					GEOSERVER_URL,
					url);
			update = true;
		}

		user = gsConfigProperties.getProperty(GEOSERVER_USER);
		if (user == null) {
			user = DEFAULT_USER;
			gsConfigProperties.setProperty(
					GEOSERVER_USER,
					user);
			update = true;
		}

		pass = gsConfigProperties.getProperty(GEOSERVER_PASS);
		if (pass == null) {
			pass = DEFAULT_PASS;
			gsConfigProperties.setProperty(
					GEOSERVER_PASS,
					pass);
			update = true;
		}
		else {
			try {
				// if password in config props is encrypted, need to decrypt it
				pass = SecurityUtils.decryptHexEncodedValue(pass);
			}
			catch (Exception e) {
				LOGGER.error(
						"An error occurred decrypting password: " + e.getLocalizedMessage(),
						e);
			}
		}

		workspace = gsConfigProperties.getProperty(GEOSERVER_WORKSPACE);
		if (workspace == null) {
			workspace = DEFAULT_WORKSPACE;
			gsConfigProperties.setProperty(
					GEOSERVER_WORKSPACE,
					workspace);
			update = true;
		}

		if (update) {
			ConfigOptions.writeProperties(
					propFile,
					gsConfigProperties);

			System.out.println("GeoServer Config Saved");
		}
	}

	/**
	 * Secondary no-arg constructor for direct-access testing
	 */
	public GeoServerConfig() {
		this(
				new File(
						System.getProperty("user.home") + File.separator + ConfigOptions.GEOWAVE_CACHE_PATH
								+ File.separator + ConfigOptions.GEOWAVE_CACHE_FILE));
	}

	public String getUrl() {
		if (url.contains("//")) {
			// assume exact URL
			return url;
		}
		return "http://" + url + "/geoserver";
	}

	public void setUrl(
			String url ) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(
			String user ) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(
			String pass ) {
		this.pass = pass;
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			String workspace ) {
		this.workspace = workspace;
	}

	public File getPropFile() {
		return propFile;
	}

	public Properties getGsConfigProperties() {
		return gsConfigProperties;
	}
}