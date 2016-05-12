package mil.nga.giat.geowave.core.cli.operations.config.options;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
	public static final String CHARSET = "ISO-8859-1";

	private final static Logger LOGGER = LoggerFactory.getLogger(ConfigSection.class);

	public final static String PROPERTIES_FILE_CONTEXT = "properties-file";
	public final static String GEOWAVE_CACHE_PATH = ".geowave";
	public final static String GEOWAVE_CACHE_FILE = "config.properties";

	/**
	 * Allow the user to override the config file location
	 */
	@Parameter(names = {
		"-cf",
		"--config-file"
	}, description = "Override configuration file (default is <home>/.geowave/config.properties)")
	private String configFile;

	public ConfigOptions() {

	}

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(
			final String configFile ) {
		this.configFile = configFile;
	}

	/**
	 * The default property file is in the user's home directory, in the
	 * .geowave folder.
	 * 
	 * @return
	 */
	public static File getDefaultPropertyPath() {
		// File location
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
	 * .geowave folder.
	 * 
	 * @return
	 */
	public static File getDefaultPropertyFile() {
		final File defaultPath = getDefaultPropertyPath();
		final String version = VersionUtils.getVersion();
		final String configFile = String.format(
				"%s%s%s%s%s",
				defaultPath.getAbsolutePath(),
				File.separator,
				(version != null ? version : "unknownversion"),
				"-",
				GEOWAVE_CACHE_FILE);
		return new File(
				configFile);
	}

	/**
	 * Write the given properties to the file, and log an error if an exception
	 * occurs.
	 * 
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
			LOGGER.error("Could not find the property file.");
			return false;
		}
		catch (IOException e) {
			LOGGER.error("Exception writing property file.");
			return false;
		}
		return true;
	}

	/**
	 * This helper function will load the properties file, or return null if it
	 * can't. It's designed to be used by other commands.
	 * 
	 * @param delimiter
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
				try (Scanner s = new Scanner(
						new FileInputStream(
								configFile),
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
		catch (final FileNotFoundException e) {
			LOGGER.error(
					"Could not find property cache file: " + configFile,
					e);
			return null;
		}
		catch (final IOException e) {
			// Stop executing.
			LOGGER.error(
					"Exception reading property cache file: " + configFile,
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
