package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

public class GeoServerConfig
{
	public static final String GEOSERVER_URL = "geoserver.url";
	public static final String GEOSERVER_USER = "geoserver.user";
	public static final String GEOSERVER_PASS = "geoserver.pass";
	public static final String GEOSERVER_WORKSPACE = "geoserver.workspace";
	public static final String GEOSERVER_CS = "geoserver.coverageStore";
	public static final String GEOSERVER_DS = "geoserver.dataStore";

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

	/**
	 * Properties File holds defaults; updates config if empty.
	 * 
	 * @param propFile
	 */
	public GeoServerConfig(
			File propFile ) {
		this.propFile = propFile;

		Properties gsConfig = ConfigOptions.loadProperties(
				propFile,
				null);

		boolean update = false;

		url = gsConfig.getProperty(GEOSERVER_URL);
		if (url == null) {
			url = DEFAULT_URL;
			gsConfig.setProperty(
					GEOSERVER_URL,
					url);
			update = true;
		}

		user = gsConfig.getProperty(GEOSERVER_USER);
		if (user == null) {
			user = DEFAULT_USER;
			gsConfig.setProperty(
					GEOSERVER_USER,
					user);
			update = true;
		}

		pass = gsConfig.getProperty(GEOSERVER_PASS);
		if (pass == null) {
			pass = DEFAULT_PASS;
			gsConfig.setProperty(
					GEOSERVER_PASS,
					pass);
			update = true;
		}

		workspace = gsConfig.getProperty(GEOSERVER_WORKSPACE);
		if (workspace == null) {
			workspace = DEFAULT_WORKSPACE;
			gsConfig.setProperty(
					GEOSERVER_WORKSPACE,
					workspace);
			update = true;
		}

		if (update) {
			ConfigOptions.writeProperties(
					propFile,
					gsConfig);

			System.out.println("GeoServer Config Saved");
		}
	}

	/**
	 * Secondary no-arg constructor for direct-access testing
	 */
	public GeoServerConfig() {
		this.propFile = null;
		this.user = DEFAULT_USER;
		this.pass = DEFAULT_PASS;
		this.url = DEFAULT_URL;
		this.workspace = DEFAULT_WORKSPACE;
	}

	public String getUrl() {
		if (!url.contains("//")) {
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
}
