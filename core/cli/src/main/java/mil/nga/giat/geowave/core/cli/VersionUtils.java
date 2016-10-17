package mil.nga.giat.geowave.core.cli;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.JCommander;

public class VersionUtils
{
	private static final String BUILD_PROPERTIES_FILE_NAME = "build.properties";
	private static final String VERSION_PROPERTY_KEY = "project.version";

	public static Properties getBuildProperties() {

		final Properties props = new Properties();
		try (InputStream stream = VersionUtils.class.getClassLoader().getResourceAsStream(
				BUILD_PROPERTIES_FILE_NAME);) {

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

		final List<String> buildAndPropertyList = Arrays.asList(getBuildProperties().toString().split(
				","));

		Collections.sort(buildAndPropertyList.subList(
				1,
				buildAndPropertyList.size()));
		for (String str : buildAndPropertyList) {
			JCommander.getConsole().println(
					str);
		}
	}
}
