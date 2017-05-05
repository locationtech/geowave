/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security.utils;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.GeoWaveEncryption;

/**
 * Security utility class for simpler interfacing with
 */
public class SecurityUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SecurityUtils.class);

	private static BaseEncryption encService;
	private static final String WRAPPER = BaseEncryption.WRAPPER;

	/**
	 * Method to decrypt a value
	 * 
	 * @param value
	 *            Value to decrypt. Should be wrapped with ENC{}
	 * @param resourceLocation
	 *            Optional value to specify the location of the encryption
	 *            service resource location
	 * @return decrypted value
	 */
	public static String decryptHexEncodedValue(
			String value,
			String resourceLocation )
			throws Exception {
		LOGGER.trace("Decrypting hex-encoded value");
		if (value != null && !"".equals(value.trim())) {
			if (BaseEncryption.isProperlyWrapped(value.trim())) {
				try {
					return getEncryptionService(
							resourceLocation).decryptHexEncoded(
							value);
				}
				catch (Throwable t) {
					LOGGER.error(
							"Encountered exception during content decryption: " + t.getLocalizedMessage(),
							t);
				}
			}
			else {
				LOGGER.debug("WARNING: Value to decrypt was not propertly encoded and wrapped with " + WRAPPER
						+ ". Not decrypting value.");
				return value;
			}
		}
		else {
			LOGGER.debug("WARNING: No value specified to decrypt.");
		}
		return "";
	}

	/**
	 * Method to encrypt and hex-encode a string value
	 * 
	 * @param value
	 *            value to encrypt and hex-encode
	 * @param resourceLocation
	 *            resource token to use for encrypting the value
	 * @return If encryption is successful, encrypted and hex-encoded string
	 *         value is returned wrapped with ENC{}
	 */
	public static String encryptAndHexEncodeValue(
			String value,
			String resourceLocation )
			throws Exception {
		LOGGER.debug("Encrypting and hex-encoding value");
		if (value != null && !"".equals(value.trim())) {
			if (!BaseEncryption.isProperlyWrapped(value)) {
				try {
					return getEncryptionService(
							resourceLocation).encryptAndHexEncode(
							value);
				}
				catch (Throwable t) {
					LOGGER.error(
							"Encountered exception during content encryption: " + t.getLocalizedMessage(),
							t);
				}
			}
			else {
				LOGGER.debug("WARNING: Value to encrypt already appears to be encrypted and already wrapped with "
						+ WRAPPER + ". Not encrypting value.");
				return value;
			}
		}
		else {
			LOGGER.debug("WARNING: No value specified to encrypt.");
			return value;
		}
		return value;
	}

	/**
	 * Returns an instance of the encryption service, initialized with the token
	 * at the provided resource location
	 * 
	 * @param resourceLocation
	 *            location of the resource token to initialize the encryption
	 *            service with
	 * @return An initialized instance of the encryption service
	 * @throws Exception
	 */
	private static synchronized BaseEncryption getEncryptionService(
			String resourceLocation )
			throws Throwable {
		if (encService == null) {
			if (resourceLocation != null && !"".equals(resourceLocation.trim())) {
				LOGGER.trace("Setting resource location for encryption service: [" + resourceLocation + "]");
				encService = new GeoWaveEncryption(
						resourceLocation);
			}
			else {
				encService = new GeoWaveEncryption();
			}
		}
		else {
			if (!resourceLocation.equals(encService.getResourceLocation())) {
				encService = new GeoWaveEncryption(
						resourceLocation);
			}
		}
		return encService;
	}

	/**
	 * Utility method to format the file path for the token key file associated
	 * with a specific parent directory
	 * 
	 * @param parentDir
	 *            Parent directory where token file is (or will be) stored
	 * @return Token key file associated with parent directory
	 */
	public static File getFormattedTokenKeyFileForParentDir(
			File parentDir ) {
		return new File(
				// get the resource location
				parentDir,
				// get the formatted token file name with version
				BaseEncryption.getFormattedTokenFileName());
	}

	/**
	 * Utilty method to format the file path for the token key file associated
	 * with a config file
	 * 
	 * @param configFile
	 *            Location of config file that token key file is associated with
	 * @return File for given config file
	 */
	public static File getFormattedTokenKeyFileForConfig(
			File configFile ) {
		// get the parent directory for the config properties file
		return getFormattedTokenKeyFileForParentDir(configFile.getParentFile());
	}
}
