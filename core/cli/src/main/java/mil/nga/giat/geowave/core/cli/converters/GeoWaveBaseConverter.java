/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.converters.BaseConverter;
import com.beust.jcommander.internal.Console;
import com.beust.jcommander.internal.DefaultConsole;
import com.beust.jcommander.internal.JDK6Console;

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
	private static Console console;
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

	protected static Console getConsole() {
		LOGGER.trace("ENTER :: getConsole()");
		if (console == null) {
			try {
				Method consoleMethod = System.class.getDeclaredMethod("console");
				Object consoleObj = consoleMethod.invoke(null);
				console = new JDK6Console(
						consoleObj);
			}
			catch (Throwable t) {
				LOGGER.error(
						"An error occurred getting declared method console. Defaulting to default console. Error message: "
								+ t.getLocalizedMessage(),
						t);
				console = new DefaultConsole();
			}
		}
		return console;
	}

	/**
	 * Prompt a user for a standard value and return the input
	 * 
	 * @param promptMessage
	 * @return
	 */
	public static String promptAndReadValue(
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
		getConsole().print(
				promptMessage);
		char[] passwordChars = getConsole().readPassword(
				defaultEchoEnabled);
		return new String(
				passwordChars);
	}

	/**
	 * Prompt a user for a password and return the input
	 * 
	 * @param promptMessage
	 * @return
	 */
	public static String promptAndReadPassword(
			String promptMessage ) {
		LOGGER.trace("ENTER :: promptAndReadPassword()");
		PropertiesUtils propsUtils = new PropertiesUtils(
				getProperties());
		boolean defaultEchoEnabled = propsUtils.getBoolean(
				Constants.CONSOLE_DEFAULT_ECHO_ENABLED_KEY,
				false);
		boolean passwordEchoEnabled = propsUtils.getBoolean(
				Constants.CONSOLE_PASSWORD_ECHO_ENABLED_KEY,
				defaultEchoEnabled);
		LOGGER.debug(
				"Password console echo is {}",
				new Object[] {
					passwordEchoEnabled ? "enabled" : "disabled"
				});
		getConsole().print(
				promptMessage);
		char[] passwordChars = getConsole().readPassword(
				passwordEchoEnabled);
		return new String(
				passwordChars);
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