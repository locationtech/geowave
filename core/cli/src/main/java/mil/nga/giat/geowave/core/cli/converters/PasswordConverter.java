/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.utils.FileUtils;
import mil.nga.giat.geowave.core.cli.utils.PropertiesUtils;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

/**
 * This class will allow support for user's passing in passwords through a
 * variety of ways. Current supported options for passwords include standard
 * password input (pass), an environment variable (env), a file containing the
 * password text (file), a properties file containing the password associated
 * with a specific key (propfile), and the user being prompted to enter the
 * password at command line (stdin). <br/>
 * <br/>
 * Required notation for specifying varying inputs are:
 * <ul>
 * <li><b>pass</b>:&lt;password&gt;</li>
 * <li><b>env</b>:&lt;variable containing the password&gt;</li>
 * <li><b>file</b>:&lt;local file containing the password&gt;</li>
 * <li><b>propfile</b>:&lt;local properties file containing the
 * password&gt;<b>:</b>&lt;property file key&gt;</li>
 * <li><b>stdin</b></li>
 * </ul>
 */
public class PasswordConverter extends
		GeoWaveBaseConverter<String>
{
	public PasswordConverter(
			String optionName ) {
		super(
				optionName);
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(PasswordConverter.class);
	public static final String STDIN = "stdin";
	private static final String SEPARATOR = ":";

	private enum KeyType {
		PASS(
				"pass" + SEPARATOR) {
			@Override
			String process(
					String password ) {
				return decryptValue(password);
			}
		},
		ENV(
				"env" + SEPARATOR) {
			@Override
			String process(
					String envVariable ) {
				return decryptValue(System.getenv(envVariable));
			}
		},
		FILE(
				"file" + SEPARATOR) {
			@Override
			String process(
					String value ) {
				try {
					String password = FileUtils.readFileContent(new File(
							value));
					if (password != null && !"".equals(password.trim())) {
						return decryptValue(password);
					}
				}
				catch (Exception ex) {
					throw new ParameterException(
							ex);
				}
				return null;
			}
		},
		PROPFILE(
				"propfile" + SEPARATOR) {
			@Override
			String process(
					String value ) {
				if (value != null && !"".equals(value.trim())) {
					if (value.indexOf(SEPARATOR) != -1) {
						String propertyFilePath = value.split(SEPARATOR)[0];
						String propertyKey = value.split(SEPARATOR)[1];
						if (propertyFilePath != null && !"".equals(propertyFilePath.trim())) {
							propertyFilePath = propertyFilePath.trim();
							File propsFile = new File(
									propertyFilePath);
							if (propsFile != null && propsFile.exists()) {
								Properties properties = PropertiesUtils.fromFile(propsFile);
								if (propertyKey != null && !"".equals(propertyKey.trim())) {
									propertyKey = propertyKey.trim();
								}
								if (properties != null && properties.containsKey(propertyKey)) {
									return decryptValue(properties.getProperty(propertyKey));
								}
							}
							else {
								try {
									throw new ParameterException(
											new FileNotFoundException(
													propsFile != null ? "Properties file not found at path: "
															+ propsFile.getCanonicalPath()
															: "No properties file specified"));
								}
								catch (IOException e) {
									throw new ParameterException(
											e);
								}
							}
						}
						else {
							throw new ParameterException(
									"No properties file path specified");
						}
					}
					else {
						throw new ParameterException(
								"Property File values are expected in input format <property file path>::<property key>");
					}
				}
				else {
					throw new ParameterException(
							new Exception(
									"No properties file specified"));
				}
				return value;
			}
		},
		STDIN(
				PasswordConverter.STDIN) {
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
					input = promptAndReadPassword("Enter password: ");
				}
				return input;
			}
		},
		DEFAULT(
				"") {
			@Override
			String process(
					String password ) {
				return decryptValue(password);
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
		for (KeyType keyType : KeyType.values()) {
			if (keyType.matches(value)) {
				String convertedValue = keyType.convert(value);
				// update the value in the configs properties file
				if (updatePasswordInConfigs() && getPropertyKey() != null && !"".equals(getPropertyKey().trim())) {
					Properties configProps = getGeoWaveConfigProperties();
					if (configProps != null) {
						// encrypt the value to be stored in the properties
						convertedValue = encryptValue(convertedValue);
						configProps.put(
								getPropertyKey(),
								convertedValue);
						ConfigOptions.writeProperties(
								getGeoWaveConfigFile(),
								configProps);
						LOGGER.debug(
								"Configuration properties successfully updated with property [{}]",
								getPropertyKey());
					}
				}
				return convertedValue;
			}
		}
		return value;
	}

	@Override
	public boolean isPassword() {
		return true;
	}

	@Override
	public boolean isRequired() {
		return true;
	}

	protected Properties getGeoWaveConfigProperties() {
		File geowaveConfigPropsFile = getGeoWaveConfigFile();
		return ConfigOptions.loadProperties(
				geowaveConfigPropsFile,
				null);
	}

	protected File getGeoWaveConfigFile() {
		return ConfigOptions.getDefaultPropertyFile();
	}
}