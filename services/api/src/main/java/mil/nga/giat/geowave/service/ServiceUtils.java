package mil.nga.giat.geowave.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceUtils
{
	private final static Logger log = LoggerFactory.getLogger(ServiceUtils.class);

	public static Properties loadProperties(
			final InputStream is ) {
		final Properties props = new Properties();
		if (is != null) {
			try {
				props.load(is);
			}
			catch (final IOException e) {
				log.error("Could not load properties from InputStream");
			}
		}
		return props;
	}

	public static String getProperty(
			final Properties props,
			final String name ) {
		if (System.getProperty(name) != null) {
			return System.getProperty(name);
		}
		else if (props.containsKey(name)) {
			return props.getProperty(name);
		}
		else {
			return null;
		}
	}
}
