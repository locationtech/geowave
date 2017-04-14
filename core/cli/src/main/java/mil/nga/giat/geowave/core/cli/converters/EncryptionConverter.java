/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class EncryptionConverter extends
		GeoWaveBaseConverter<String>
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
					input = promptAndReadPassword("Enter value to encrypt: ");
				}
				return input;
			}
		};

		String prefix;

		private KeyType(
				String prefix ) {
			this.prefix = prefix;
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
		LOGGER.trace("ENTER :: convert()");
		for (KeyType keyType : KeyType.values()) {
			if (keyType.matches(value)) {
				return keyType.convert(value);
			}
		}
		return value;
	}

	@Override
	public boolean isPassword() {
		return false;
	}

	@Override
	public boolean isRequired() {
		return true;
	}
}