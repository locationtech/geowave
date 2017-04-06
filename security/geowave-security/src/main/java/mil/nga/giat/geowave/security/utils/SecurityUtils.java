/**
 * 
 */
package mil.nga.giat.geowave.security.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.security.crypto.impl.GeoWaveEncryptionService;

/**
 * Security utility class for simpler interfacing with
 */
public class SecurityUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SecurityUtils.class);

	private static GeoWaveEncryptionService encService;
	private static String resourceLocation;
	private static final String WRAPPER = GeoWaveEncryptionService.WRAPPER;

	static {
		resourceLocation = new GeoWaveEncryptionService().getResourceLocation();
	}

	/**
	 * 
	 * @return
	 */
	public static String getResourceLocation() {
		return resourceLocation;
	}

	/**
	 * 
	 * @param resourceLocation
	 */
	public static void setResourceLocation(
			String resourceLoc ) {
		resourceLocation = resourceLoc;
	}

	/**
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static String decryptValue(
			byte[] value )
			throws Exception {
		return decryptValue(
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
	 * @throws Exception
	 */
	public static String decryptValue(
			byte[] value,
			String resourceLocation )
			throws Exception {

		String strValue = new String(
				value,
				"UTF-8");
		if (strValue != null && !"".equals(strValue.trim())) {
			LOGGER.trace("Decrypting base64-encoded value: [" + strValue + "]");
			if (getEncryptionService(
					resourceLocation).isProperlyWrapped(
					strValue.trim())) {
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

	public static String decryptHexEncodedValue(
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
	public static String decryptHexEncodedValue(
			String value,
			String resourceLocation )
			throws Exception {
		String decryptedValue = "";
		LOGGER.trace("Decrypting hex-encoded value: [" + value + "]");

		if (value != null && !"".equals(value.trim())) {
			if (getEncryptionService(
					resourceLocation).isProperlyWrapped(
					value.trim())) {
				try {
					decryptedValue = getEncryptionService(
							resourceLocation).decryptHexEncoded(
							value);
				}
				catch (RuntimeException re) {
					LOGGER.error(
							"Encountered RuntimeException during content decryption: " + re.getLocalizedMessage(),
							re);
				}
				catch (Exception e) {
					LOGGER.error(
							"Encountered Exception during content decryption: " + e.getLocalizedMessage(),
							e);
				}
				catch (Throwable e) {
					LOGGER.error(
							"Encountered Throwable during content decryption: " + e.getLocalizedMessage(),
							e);
				}
			}
			else {
				LOGGER.debug("WARNING: Value to decrypt was not propertly encoded and wrapped with " + WRAPPER
						+ ". Not decrypting value.");
				decryptedValue = value;
			}
		}
		else {
			LOGGER.debug("WARNING: No value specified to decrypt.");
			decryptedValue = value;
		}
		return decryptedValue;
	}

	/**
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static byte[] encryptValue(
			String value )
			throws Exception {
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
	public static byte[] encryptValue(
			String value,
			String resourceLocation )
			throws Exception {
		byte[] bytes = null;
		if ((value != null) && (!"".equals(value.trim()))) {
			LOGGER.trace("Encrypting and base64-encoding value: [" + value + "]");
			if (!getEncryptionService(
					resourceLocation).isProperlyWrapped(
					value)) {
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
	public static String encryptAndHexEncodeValue(
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
	public static String encryptAndHexEncodeValue(
			String value,
			String resourceLocation )
			throws Exception {
		String encryptedValue = "";
		LOGGER.info("Encrypting and hex-encoding value: [" + value + "]");
		if (value != null && !"".equals(value.trim())) {
			if (!getEncryptionService(
					resourceLocation).isProperlyWrapped(
					value)) {
				try {
					encryptedValue = getEncryptionService(
							resourceLocation).encryptAndHexEncode(
							value);
				}
				catch (RuntimeException re) {
					LOGGER.error(
							"Encountered RuntimeException during content encryption: " + re.getLocalizedMessage(),
							re);
				}
				catch (Exception e) {
					LOGGER.error(
							"Encountered Exception during content encryption: " + e.getLocalizedMessage(),
							e);
				}
				catch (Throwable e) {
					LOGGER.error(
							"Encountered Throwable during content encryption: " + e.getLocalizedMessage(),
							e);
				}
			}
			else {
				LOGGER.debug("WARNING: Value to encrypt already appears to be encrypted and already wrapped with "
						+ WRAPPER + ". Not encrypting value.");
				encryptedValue = value;
			}
		}
		else {
			LOGGER.debug("WARNING: No value specified to encrypt.");
			encryptedValue = value;
		}
		return encryptedValue;
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
	private static GeoWaveEncryptionService getEncryptionService(
			String resourceLocation )
			throws Exception {
		if (encService == null) {
			encService = new GeoWaveEncryptionService();
			try {
				if (resourceLocation != null && !"".equals(resourceLocation.trim())) {
					LOGGER.trace("Setting resource location for encryption service: [" + resourceLocation + "]");
					encService.setResourceLocation(resourceLocation);
				}
			}
			catch (IllegalArgumentException e) {
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

	/**
	 * Main method for providing ability to encrypt or decrypt values based on
	 * encoding (base64 or hex)
	 * 
	 * @param args
	 *            - 3 arguments expected - operation, value to encrypt/decrypt,
	 *            location of resource token
	 * @throws Exception
	 */
	public static void main(
			String[] args )
			throws Exception {
		final String decryptHexValueKey = "-dh";
		final String encryptHexValueKey = "-eh";
		final String newTokenValueKey = "-new_token";

		String description = "SecurityUtils: Provides ability to generate a new encryption token, encrypt or decrypt a hex-or-base64 string value.\n"
				+ newTokenValueKey
				+ " - generates a new resource token\n"
				+ decryptHexValueKey
				+ " <value> <resource token location> - decrypts hex-encoded value using resource token\n"
				+ encryptHexValueKey
				+ " <value> <resource token location> - encrypts and hex encodes value using resource token\n";

		String operation = null, value = null, resourceLocation = null;

		if (args.length > 0) {
			if (args.length != 0) {
				operation = args[0];
			}
			if (args.length > 1) {
				value = args[1];
			}
			if (args.length > 2) {
				resourceLocation = args[2];
			}

			// new token operation requires no other inputs
			if (operation != null && operation.equals(newTokenValueKey)) {
				String newTokenValue = GeoWaveEncryptionService.generateRandomSecretKey();
				System.out.println(newTokenValue);
			}
			else if (operation != null && !"".equals(operation) && value != null && !"".equals(value)) {
				if (decryptHexValueKey.equals(operation)) {
					System.out.println("Decrypting hex-encoded value using "
							+ ((resourceLocation == null) ? "default token" : resourceLocation));
					String decrypted = decryptHexEncodedValue(
							value,
							resourceLocation);
					System.out.println(decrypted);
				}
				else if (encryptHexValueKey.equals(operation)) {
					System.out.println("Encrypting and hex-encoding value using "
							+ ((resourceLocation == null) ? "default token" : resourceLocation));
					String encrypted = encryptAndHexEncodeValue(
							value,
							resourceLocation);
					System.out.println(encrypted);
				}
				else {
					System.out.println("Invalid argument specified. \n" + description);
				}
			}
			else {
				System.out.println(description);
			}
		}
		else {
			System.out.println(description);
		}
	}
}