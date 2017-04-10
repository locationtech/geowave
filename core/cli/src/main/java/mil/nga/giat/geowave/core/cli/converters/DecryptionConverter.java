/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.internal.Console;
import com.beust.jcommander.internal.DefaultConsole;
import com.beust.jcommander.internal.JDK6Console;

import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

/**
 * This class will allow support for user's passing in values to be decrypted
 * through a variety of ways. Current supported options for values include the
 * user being prompted to enter the value at command line (stdin), or through a
 * clear text input. <br/>
 * <br/>
 * Required notation for specifying varying inputs are:
 * <ul>
 * <li><b>stdin</b></li>
 * </ul>
 */
public class DecryptionConverter implements
		IStringConverter<String>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PasswordConverter.class);
	public static final String STDIN = "stdin";

	private enum KeyType {
		DEFAULT(
				"") {

			private String input = null;

			@Override
			public boolean matches(
					String value ) {
				return prefix.equals(value);
			}

			@Override
			String process(
					String value ) {
				if (input == null) {
					Console console = getConsole();
					console.print("Enter value to decrypt: ");
					char[] passwordChars = console.readPassword(false);
					String password = new String(
							passwordChars);
					input = decryptPassword(password);
				}
				return input;
			}
		};

		String prefix;

		private KeyType(
				String prefix ) {
			this.prefix = prefix;
		}

		private Console console;

		public Console getConsole() {
			if (console == null) {
				try {
					Method consoleMethod = System.class.getDeclaredMethod("console");
					Object consoleObj = consoleMethod.invoke(null);
					console = new JDK6Console(
							consoleObj);
				}
				catch (Throwable t) {
					console = new DefaultConsole();
				}
			}
			return console;
		}

		protected String decryptPassword(
				String password ) {
			if (password != null) {
				try {
					return new SecurityUtils().decryptHexEncodedValue(password);
				}
				catch (Exception e) {
					LOGGER.error(
							"An error occurred decrypting the provided password value: [" + e.getLocalizedMessage()
									+ "]",
							e);
				}
			}
			return password;
		}

		public boolean matches(
				String value ) {
			return value.startsWith(prefix);
		}

		public String convert(
				String value ) {
			return process(value.substring(prefix.length()));
		}

		String process(
				String value ) {
			return value;
		}
	}

	@Override
	public String convert(
			String value ) {
		for (KeyType keyType : KeyType.values()) {
			if (keyType.matches(value)) {
				return keyType.convert(value);
			}
		}
		return value;
	}
}
