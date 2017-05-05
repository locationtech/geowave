/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.converters.BaseConverter;

import jline.console.ConsoleReader;
import mil.nga.giat.geowave.core.cli.Constants;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.utils.PropertiesUtils;

/**
 * Base value converter for handling field conversions of varying types
 * 
 * @param <T>
 */
public abstract class GeoWaveBaseConverter<T> extends
		BaseConverter<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveBaseConverter.class);

	private String propertyKey;
	private static Properties properties;

	public GeoWaveBaseConverter() {
		super(
				"");
		init();
	}

	public GeoWaveBaseConverter(
			String optionName ) {
		super(
				optionName);
		init();
	}

	private void init() {
		File propertyFile = null;
		if (new ConfigOptions().getConfigFile() != null) {
			propertyFile = new File(
					new ConfigOptions().getConfigFile());
		}
		else {
			propertyFile = ConfigOptions.getDefaultPropertyFile();
		}
		if (propertyFile != null && propertyFile.exists()) {
			setProperties(ConfigOptions.loadProperties(
					propertyFile,
					null));
		}
	}

	/**
	 * Prompt a user for a standard value and return the input
	 * 
	 * @param promptMessage
	 * @return
	 */
	protected static String promptAndReadValue(
			String promptMessage ) {
		LOGGER.trace("ENTER :: promptAndReadValue()");
		PropertiesUtils propsUtils = new PropertiesUtils(
				getProperties());
		boolean defaultEchoEnabled = propsUtils.getBoolean(
				Constants.CONSOLE_DEFAULT_ECHO_ENABLED_KEY,
				false);
		LOGGER.debug(
				"Default console echo is {}",
				new Object[] {
					defaultEchoEnabled ? "enabled" : "disabled"
				});

		String value = null;
		try {
			ConsoleReader reader = new ConsoleReader();
			if (defaultEchoEnabled) {
				value = reader.readLine(promptMessage);
			}
			else {
				value = reader.readLine(
						promptMessage,
						'*');
			}
		}
		catch (IOException e) {
			LOGGER.error(
					"An error occurred reading value from console: " + e.getLocalizedMessage(),
					e);
		}
		return value;
	}

	/**
	 * Prompt a user for a password and return the input
	 * 
	 * @param promptMessage
	 * @return
	 */
	protected static String promptAndReadPassword(
			String promptMessage ) {
		LOGGER.trace("ENTER :: promptAndReadPassword()");
		PropertiesUtils propsUtils = new PropertiesUtils(
				getProperties());
		boolean defaultEchoEnabled = false, passwordEchoEnabled = false;
		if (propsUtils != null) {
			defaultEchoEnabled = propsUtils.getBoolean(
					Constants.CONSOLE_DEFAULT_ECHO_ENABLED_KEY,
					false);
			passwordEchoEnabled = propsUtils.getBoolean(
					Constants.CONSOLE_PASSWORD_ECHO_ENABLED_KEY,
					defaultEchoEnabled);
		}
		LOGGER.debug(
				"Password console echo is {}",
				new Object[] {
					passwordEchoEnabled ? "enabled" : "disabled"
				});

		String password = null;
		try {
			ConsoleReader reader = new ConsoleReader();
			if (passwordEchoEnabled) {
				password = reader.readLine(promptMessage);
			}
			else {
				password = reader.readLine(
						promptMessage,
						'*');
			}
		}
		catch (IOException e) {
			LOGGER.error(
					"An error occurred reading password from console: " + e.getLocalizedMessage(),
					e);
		}
		return password;
	}

	/**
	 * @return the propertyKey
	 */
	public String getPropertyKey() {
		return propertyKey;
	}

	/**
	 * @param propertyKey
	 *            the propertyKey to set
	 */
	public void setPropertyKey(
			String propertyKey ) {
		this.propertyKey = propertyKey;
	}

	/**
	 * Specify if a converter is for a password field. This allows a password
	 * field to be specified, though side-stepping most of the default
	 * jcommander password functionality
	 * 
	 * @return
	 */
	public boolean isPassword() {
		return false;
	}

	/**
	 * Specify if a field is required
	 * 
	 * @return
	 */
	public boolean isRequired() {
		return false;
	}

	/**
	 * Specify if converter should update the config properties with the
	 * specified value. If converter is for a password, value is true, otherwise
	 * false.
	 * 
	 * @return
	 */
	public boolean updatePasswordInConfigs() {
		return isPassword();
	}

	/**
	 * @return the properties
	 */
	private static Properties getProperties() {
		return properties;
	}

	/**
	 * @param properties
	 *            the properties to set
	 */
	private void setProperties(
			Properties props ) {
		properties = props;
	}
}
