/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.GeoWaveEncryption;
import net.jcip.annotations.ThreadSafe;

/**
 * Security utility class for simpler interfacing with
 */
@ThreadSafe
public class SecurityUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SecurityUtils.class);

	private BaseEncryption encService;
	private String resourceLocation;
	private static final String WRAPPER = BaseEncryption.WRAPPER;

	public SecurityUtils() {
		resourceLocation = new GeoWaveEncryption().getResourceLocation();
	}

	/**
	 * 
	 * @return
	 */
	public String getResourceLocation() {
		return resourceLocation;
	}

	/**
	 * 
	 * @param resourceLocation
	 */
	public void setResourceLocation(
			String resourceLoc ) {
		resourceLocation = resourceLoc;
	}

	/**
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public String decryptValue(
			byte[] value )
			throws Throwable {
		synchronized (value) {
			return decryptValue(
					value,
					getResourceLocation());
		}
	}

	/**
	 * Method to decrypt a value
	 * 
	 * @param value
	 *            Value to decrypt. Should be wrapped with ENC{}
	 * @param resourceLocation
	 *            Optional value to specify the location of the encryption
	 *            service resource location
	 * @return decrypted value
	 * @throws Exception
	 */
	public String decryptValue(
			byte[] value,
			String resourceLocation )
			throws Throwable {
		synchronized (value) {
			String strValue = new String(
					value,
					"UTF-8");
			if (strValue != null && !"".equals(strValue.trim())) {
				LOGGER.trace("Decrypting base64-encoded value");
				if (BaseEncryption.isProperlyWrapped(strValue.trim())) {
					return new String(
							getEncryptionService(
									resourceLocation).decrypt(
									value,
									true),
							"UTF-8");
				}
				else {
					LOGGER.debug("WARNING: Value to decrypt was not propertly encoded and wrapped with " + WRAPPER
							+ ". Not decrypting value.");
					return strValue;
				}
			}
			else {
				LOGGER.debug("WARNING: No value specified to decrypt.");
				return strValue;
			}
		}
	}

	public String decryptHexEncodedValue(
			String value )
			throws Exception {
		return decryptHexEncodedValue(
				value,
				getResourceLocation());
	}

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
	public String decryptHexEncodedValue(
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
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public byte[] encryptValue(
			String value )
			throws Throwable {
		return encryptValue(
				value,
				getResourceLocation());
	}

	/**
	 * Method to encrypt a value
	 * 
	 * @param value
	 *            Value to encrypt
	 * @param resourceLocation
	 *            Optional value to specify the location of the encryption
	 *            service resource location
	 * @return encrypted value
	 * @throws Exception
	 */
	public byte[] encryptValue(
			String value,
			String resourceLocation )
			throws Throwable {
		byte[] bytes = null;
		if ((value != null) && (!"".equals(value.trim()))) {
			LOGGER.trace("Encrypting and base64-encoding value");
			if (!BaseEncryption.isProperlyWrapped(value)) {
				bytes = getEncryptionService(
						resourceLocation).encrypt(
						value.getBytes("UTF-8"));
			}
			else {
				LOGGER.debug("WARNING: Value to encrypt already appears to be encrypted and already wrapped with "
						+ WRAPPER + ". Not encrypting value.");
				bytes = value.getBytes("UTF-8");
			}
		}
		else {
			LOGGER.debug("WARNING: No value specified to encrypt.");
		}
		return bytes;
	}

	/**
	 * Method to encrypt and hex-encode a string value
	 * 
	 * @param value
	 *            value to encrypt and hex-encode
	 * @return If encryption is successful, encrypted and hex-encoded string
	 *         value is returned wrapped with ENC{}
	 * @throws Exception
	 */
	public String encryptAndHexEncodeValue(
			String value )
			throws Exception {
		return encryptAndHexEncodeValue(
				value,
				getResourceLocation());
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
	public String encryptAndHexEncodeValue(
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
					t.printStackTrace();
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
		return "";
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
	private BaseEncryption getEncryptionService(
			String resourceLocation )
			throws Throwable {
		if (encService == null) {
			encService = new GeoWaveEncryption();
			try {
				if (resourceLocation != null && !"".equals(resourceLocation.trim())) {
					LOGGER.trace("Setting resource location for encryption service: [" + resourceLocation + "]");
					encService.setResourceLocation(resourceLocation);
				}
			}
			catch (IllegalArgumentException e) {
				e.printStackTrace();
				LOGGER.error(
						"Encountered IllegalArgumentException getting encryption service: " + e.getLocalizedMessage(),
						e);
			}
			catch (Exception e) {
				LOGGER.error(
						"Encountered Exception getting encryption service: " + e.getLocalizedMessage(),
						e);
			}
		}
		else {
			if (!resourceLocation.equals(encService.getResourceLocation())) {
				encService.setResourceLocation(resourceLocation);
			}
		}
		return encService;
	}

	/**
	 * Method to base64 encode an input value
	 * 
	 * @param input
	 *            value to base64 encode
	 * @return If successful, returns a base64-encoded value
	 */
	public static String base64Encode(
			String input ) {
		Base64 base64 = new Base64(
				Integer.MAX_VALUE,
				new byte[] {});
		try {
			return base64.encodeToString(input.getBytes("UTF-8"));
		}
		catch (UnsupportedEncodingException e) {
			LOGGER.error(
					"Encountered UnsupportedEncodingException: " + e.getLocalizedMessage(),
					e);
		}
		return input;
	}

	/**
	 * Method to return a hashed binary value in string representation
	 * 
	 * @param md5Bytes
	 *            binary value to convert
	 * @return If successful, returns a hashed binary value in string
	 *         representation
	 */
	public static String convertHashToString(
			byte[] md5Bytes ) {
		StringBuilder returnVal = new StringBuilder();
		for (int i = 0; i < md5Bytes.length; i++)
			// convert it to a hash value
			returnVal.append(Integer.toString(
					(md5Bytes[i] & 0xff) + 0x100,
					16).substring(
					1));
		return returnVal.toString();
	}

	/**
	 * Generate MD5 Hash
	 * 
	 * @param content
	 *            Binary content to generate an MD5 hash of
	 * @return hashed value of specified binary content
	 * @throws NoSuchAlgorithmException
	 */
	public static String getMD5Hash(
			byte[] content )
			throws NoSuchAlgorithmException {
		MessageDigest hasher = MessageDigest.getInstance("MD5");
		hasher.update(content);
		byte[] md5Bytes = hasher.digest();
		return convertHashToString(md5Bytes);
	}

	public static String generateNewToken()
			throws Exception {
		return BaseEncryption.generateRandomSecretKey();
	}
}