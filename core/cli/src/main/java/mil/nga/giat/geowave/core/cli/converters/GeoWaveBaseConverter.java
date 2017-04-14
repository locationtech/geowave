/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.converters.BaseConverter;
import com.beust.jcommander.internal.Console;
import com.beust.jcommander.internal.DefaultConsole;
import com.beust.jcommander.internal.JDK6Console;

import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

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

	public GeoWaveBaseConverter() {
		super(
				"");
	}

	public GeoWaveBaseConverter(
			String optionName ) {
		super(
				optionName);
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
	 * Prompt a user for a password and return the input
	 * 
	 * @param promptMessage
	 * @return
	 */
	protected static String promptAndReadPassword(
			String promptMessage ) {
		LOGGER.trace("ENTER :: promptAndReadPassword()");
		getConsole().print(
				promptMessage);
		char[] passwordChars = getConsole().readPassword(
				false);
		String password = new String(
				passwordChars);
		return decryptValue(password);
	}

	protected static String encryptValue(
			String value ) {
		if (value != null) {
			try {
				return new SecurityUtils().encryptAndHexEncodeValue(value);
			}
			catch (Exception e) {
				LOGGER.error(
						"An error occurred decrypting the provided value: [" + e.getLocalizedMessage() + "]",
						e);
			}
		}
		return value;
	}

	protected static String decryptValue(
			String value ) {
		if (value != null) {
			try {
				return new SecurityUtils().decryptHexEncodedValue(value);
			}
			catch (Exception e) {
				LOGGER.error(
						"An error occurred decrypting the provided value: [" + e.getLocalizedMessage() + "]",
						e);
			}
		}
		return value;
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
}
