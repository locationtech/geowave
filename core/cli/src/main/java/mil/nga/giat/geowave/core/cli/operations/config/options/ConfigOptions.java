package mil.nga.giat.geowave.core.cli.operations.config.options;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;

/**
 * Config options allows the user to override the default location for
 * configuration options, and also allows commands to load the properties needed
 * for running the program.
 */
public class ConfigOptions
{
	/** Character set to load properties files with */
	public static final String CHARSET = "ISO-8859-1";

	private final static Logger LOGGER = LoggerFactory.getLogger(ConfigSection.class);

	/** Name of context object for the properties */
	public final static String PROPERTIES_FILE_CONTEXT = "properties-file";
	/** Default GeoWave home directory path */
	public final static String GEOWAVE_CACHE_PATH = ".geowave";
	/**
	 * Default configuration properties file name. File name will be formatted
	 * to include the current GeoWave version
	 */
	public final static String GEOWAVE_CACHE_FILE = "config.properties";

	/**
	 * Allow the user to override the config file location
	 */
	@Parameter(names = {
		"-cf",
		"--config-file"
	}, description = "Override configuration file (default is <home>/.geowave/config.properties)")
	private String configFile;

	/**
	 * Base constructor
	 */
	public ConfigOptions() {}

	/**
	 * Return configuration file
	 * 
	 * @return configuration file
	 */
	public String getConfigFile() {
		return configFile;
	}

	/**
	 * Set a new configuration file
	 * 
	 * @param configFile
	 *            configuration file to use
	 */
	public void setConfigFile(
			final String configFile ) {
		this.configFile = configFile;
	}

	/**
	 * The default property file is in the user's home directory, in the
	 * .geowave folder.
	 * 
	 * @return File object associated with the default property configuration
	 *         path
	 */
	public static File getDefaultPropertyPath() {
		// File location
		// HP Fortify "Path Manipulation" false positive
		// What Fortify considers "user input" comes only
		// from users with OS-level access anyway
		final String cachePath = String.format(
				"%s%s%s",
				System.getProperty("user.home"),
				File.separator,
				GEOWAVE_CACHE_PATH);
		return new File(
				cachePath);
	}

	/**
	 * The default property file is in the user's home directory, in the
	 * .geowave folder. If the version can not be found the first available
	 * property file in the folder is used
	 * 
	 * @return Default Property File
	 */
	public static File getDefaultPropertyFile() {
		// HP Fortify "Path Manipulation" false positive
		// What Fortify considers "user input" comes only
		// from users with OS-level access anyway
		final File defaultPath = getDefaultPropertyPath();
		final String version = VersionUtils.getVersion();

		if (version != null) {
			return formatConfigFile(
					version,
					defaultPath);
		}
		else {
			final String[] configFiles = defaultPath.list(new FilenameFilter() {

				@Override
				public boolean accept(
						File dir,
						String name ) {
					return name.endsWith("-config.properties");
				}
			});
			if (configFiles != null && configFiles.length > 0) {
				final String backupVersion = configFiles[0].substring(
						0,
						configFiles[0].length() - 18);
				return formatConfigFile(
						backupVersion,
						defaultPath);
			}
			else {
				return formatConfigFile(
						"unknownversion",
						defaultPath);
			}
		}
	}

	/**
	 * Configures a File based on a given path name and version
	 * 
	 * @param version
	 *            Current GeoWave version
	 * @param defaultPath
	 *            Path to config file to be configured
	 * @return Configured File
	 */
	public static File formatConfigFile(
			String version,
			File defaultPath ) {
		// HP Fortify "Path Manipulation" false positive
		// What Fortify considers "user input" comes only
		// from users with OS-level access anyway
		final String configFile = String.format(
				"%s%s%s%s%s",
				defaultPath.getAbsolutePath(),
				File.separator,
				version,
				"-",
				GEOWAVE_CACHE_FILE);
		return new File(
				configFile);
	}

	/**
	 * Write the given properties to the file, and log an error if an exception
	 * occurs.
	 * 
	 * @param configFile
	 *            File object to write properties to
	 * @param properties
	 *            Properties to write
	 * @return true if success, false if failure
	 */
	public static boolean writeProperties(
			final File configFile,
			final Properties properties ) {
		try {
			Properties tmp = new Properties() {
				private static final long serialVersionUID = 1L;

				@Override
				public Set<Object> keySet() {
					return Collections.unmodifiableSet(new TreeSet<Object>(
							super.keySet()));
				}

				@Override
				public synchronized Enumeration<Object> keys() {
					return Collections.enumeration(new TreeSet<Object>(
							super.keySet()));
				}
			};
			tmp.putAll(properties);
			try (FileOutputStream str = new FileOutputStream(
					configFile)) {
				tmp.store(
						str,
						null);
			}
		}
		catch (FileNotFoundException e) {
			LOGGER.error(
					"Could not find the property file.",
					e);
			return false;
		}
		catch (IOException e) {
			LOGGER.error(
					"Exception writing property file.",
					e);
			return false;
		}
		return true;
	}

	/**
	 * This helper function will load the properties file, or return null if it
	 * can't. It's designed to be used by other commands.
	 * 
	 * @param configFile
	 *            Configuration file to load
	 * @param pattern
	 *            Pattern to use to filter load only specific configuration
	 *            properties, with a key matching a specific naming convention
	 *            pattern. If not specified, all properties will be loaded
	 * @return A properties object containing loaded properties from specified
	 *         configuration file
	 */
	public static Properties loadProperties(
			final File configFile,
			final String pattern ) {

		Pattern p = null;
		if (pattern != null) {
			p = Pattern.compile(pattern);
		}

		// Load the properties file.
		final Properties properties = new Properties();
		InputStream is = null;
		try {
			if (p != null) {
				try (FileInputStream input = new FileInputStream(
						configFile); Scanner s = new Scanner(
						input,
						CHARSET)) {
					final ByteArrayOutputStream out = new ByteArrayOutputStream();
					final PrintWriter writer = new PrintWriter(
							new OutputStreamWriter(
									out,
									CHARSET));
					while (s.hasNext()) {
						final String line = s.nextLine();
						if (p.matcher(
								line).find()) {
							writer.println(line);
						}
					}
					writer.flush();
					is = new ByteArrayInputStream(
							out.toByteArray());
				}
			}
			else {
				is = new FileInputStream(
						configFile);
			}

			properties.load(is);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Could not find property cache file: " + configFile,
					e);

			return null;
		}
		finally {
			if (is != null) {
				try {
					is.close();
				}
				catch (IOException e) {
					LOGGER.error(
							e.getMessage(),
							e);
				}
			}
		}

		return properties;
	}

	/**
	 * Load the properties file into the input params.
	 * 
	 * @param inputParams
	 *            Arguments to be used to allow sections and commands to modify
	 *            how arguments are parsed during prepare/execute stage.
	 */
	public void prepare(
			OperationParams inputParams ) {
		File propertyFile = null;
		if (getConfigFile() != null) {
			propertyFile = new File(
					getConfigFile());
		}
		else {
			propertyFile = getDefaultPropertyFile();
		}

		// Set the properties on the context.
		inputParams.getContext().put(
				PROPERTIES_FILE_CONTEXT,
				propertyFile);
	}
}
