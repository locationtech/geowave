package mil.nga.giat.geowave.core.cli;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;

import com.beust.jcommander.JCommander;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

public class VersionUtils
{
	private static final String BUILD_PROPERTIES_FILE_NAME = "build.properties";
	private static final String VERSION_PROPERTY_KEY = "project.version";

	public static File getBuildPropertiesFile() {
		final URL buildProperties = VersionUtils.class.getClassLoader().getResource(
				BUILD_PROPERTIES_FILE_NAME);
		if (buildProperties != null) {
			final File buildPropertiesFile = new File(
					buildProperties.getFile());
			return buildPropertiesFile;
		}
		return null;
	}

	public static Properties getBuildProperties() {

		final Properties props = new Properties();
		try {
			InputStream stream = VersionUtils.class.getClassLoader().getResourceAsStream(
					BUILD_PROPERTIES_FILE_NAME);
			if (stream != null) {
				props.load(stream);
			}
			return props;
		}
		catch (final IOException e) {
			JCommander.getConsole().print(
					"Cannot read GeoWave build properties to show version information: " + e.getMessage());
		}
		return props;
	}

	public static String getVersion() {
		return getBuildProperties().getProperty(
				VERSION_PROPERTY_KEY);
	}

	public static void printVersionInfo() {
		final File buildPropertiesFile = getBuildPropertiesFile();
		if (buildPropertiesFile != null) {
			try (Scanner scanner = new Scanner(
					buildPropertiesFile,
					ConfigOptions.CHARSET)) {

				while (scanner.hasNextLine()) {
					final String line = scanner.nextLine();
					JCommander.getConsole().println(
							line);
				}

				scanner.close();
			}
			catch (final IOException e) {
				JCommander.getConsole().print(
						"Cannot read GeoWave build properties to show version information: " + e.getMessage());
			}
		}
		else {
			JCommander.getConsole().print(
					"Cannot read GeoWave build properties to show version information");
		}
	}
}
