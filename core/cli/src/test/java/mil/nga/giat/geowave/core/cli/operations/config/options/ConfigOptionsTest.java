package mil.nga.giat.geowave.core.cli.operations.config.options;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

public class ConfigOptionsTest
{

	@Test
	public void testGetPropertyPath() {
		String propertypath = ConfigOptions.getDefaultPropertyPath().getAbsolutePath();
		String expectedoutput = String.format(
				"%s%s",
				System.getProperty("user.home"),
				"/.geowave");
		assertEquals(
				expectedoutput,
				propertypath);
	}

	@Test
	public void testGetPropertyFile() {
		String propertyfile = ConfigOptions.getDefaultPropertyFile().getAbsolutePath();
		String expectedoutput = String.format(
				"%s%s",
				System.getProperty("user.home"),
				"/.geowave/unknownversion-config.properties");
		assertEquals(
				expectedoutput,
				propertyfile);
	}

	@Test
	public void testWriteProperty() {
		String parent = String.format(
				"%s",
				System.getProperty("user.home"));
		File path = new File(
				parent);
		File configfile = ConfigOptions.formatConfigFile(
				"0",
				path);
		Properties prop = new Properties();
		String key = "key";
		String value = "value";
		prop.setProperty(
				key,
				value);
		boolean success = ConfigOptions.writeProperties(
				configfile,
				prop);
		if (success) {
			Properties loadprop = ConfigOptions.loadProperties(
					configfile,
					key);
			assertEquals(
					value,
					loadprop.getProperty(key));
		}

	}

}
