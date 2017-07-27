/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security.crypto;

import java.io.File;
import java.security.Key;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import mil.nga.giat.geowave.core.cli.utils.FileUtils;

/**
 * Abstract base encryption class for setting up and defining common
 * encryption/decryption methods
 */
public abstract class BaseEncryption
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseEncryption.class);

	public static String resourceName = "geowave_crypto_key.dat";
	private String resourceLocation;
	private Key key = null;

	/*
	 * PROTECT this value. The salt value is the second-half of the protection
	 * mechanism for key used when encrypting or decrypting the content.<br/> We
	 * cannot generate a new random cryptography key each time, as that would
	 * mean two different keys.<br/> At the same time, encrypted values would be
	 * very vulnerable to unintentional exposure if (a wrong) someone got access
	 * to the token key file, so this salt allows us to protect the encryption
	 * with "2 locks" - both are needed to decrypt a value that was encrypted
	 * with the SAME two values (salt - below - and token file - specified at
	 * resourceLocation)
	 */
	protected byte[] salt = null;
	protected File tokenFile = null;

	private static final String PREFIX = "ENC{";
	private static final String SUFFIX = "}";
	public static final String WRAPPER = PREFIX + SUFFIX;
	private static final Pattern ENCCodePattern = Pattern.compile(PREFIX.replace(
			"{",
			"\\{") + "([^}]+)" + SUFFIX.replace(
			"{",
			"\\{"));

	private final String KEY_ENCRYPTION_ALGORITHM = "AES";

	/**
	 * Base constructor for encryption, allowing a resource location for the
	 * cryptography token key to be specified, rather than using the
	 * default-generated path
	 * 
	 * @param resourceLocation
	 *            Path to cryptography token key file
	 */
	public BaseEncryption(
			final String resourceLocation ) {
		try {
			setResourceLocation(resourceLocation);
			init();
		}
		catch (Throwable t) {
			LOGGER.error(
					t.getLocalizedMessage(),
					t);
		}
	}

	/**
	 * Base constructor for encryption
	 */
	public BaseEncryption() {
		init();
	}

	/**
	 * Method to initialize all required fields, check for the existence of the
	 * cryptography token key, and generate the key for encryption/decryption
	 */
	private void init() {
		try {
			checkForToken();
			setResourceLocation(tokenFile.getCanonicalPath());

			salt = "Ge0W@v3-Ro0t-K3y".getBytes("UTF-8");

			generateRootKeyFromToken();
		}
		catch (Throwable t) {
			LOGGER.error(
					t.getLocalizedMessage(),
					t);
		}
	}

	/**
	 * Check if encryption token exists. If not, create one initially
	 */
	private void checkForToken()
			throws Throwable {
		if (getResourceLocation() != null) {
			tokenFile = new File(
					getResourceLocation());
		}
		else {
			if (new ConfigOptions().getConfigFile() != null) {
				tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(new File(
						new ConfigOptions().getConfigFile()));
			}
			else {
				tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(ConfigOptions.getDefaultPropertyFile());
			}
		}
		if (tokenFile == null || !tokenFile.exists()) {
			generateNewEncryptionToken(tokenFile);
		}
	}

	/**
	 * Generates a token file resource name that includes the current version
	 * 
	 * @return formatted token key file name
	 */
	public static String getFormattedTokenFileName() {
		final String tokenFileName = resourceName.substring(
				0,
				resourceName.lastIndexOf("."));
		final String tokenFileExtension = resourceName.substring(resourceName.lastIndexOf("."));
		String formattedTokenFileName = String.format(
				"%s%s%s%s",
				VersionUtils.getVersion(),
				"-",
				tokenFileName,
				tokenFileExtension);
		return formattedTokenFileName;
	}

	/**
	 * Generate a new token value in a specified file
	 * 
	 * @param tokenFile
	 * @return
	 */
	public static boolean generateNewEncryptionToken(
			File tokenFile )
			throws Exception {
		boolean success = false;
		try {
			LOGGER.info(
					"Writing new encryption token to file at path {}",
					tokenFile.getCanonicalPath());
			org.apache.commons.io.FileUtils.writeStringToFile(
					tokenFile,
					generateRandomSecretKey());
			LOGGER.info("Completed writing new encryption token to file");
			success = true;
		}
		catch (Exception ex) {
			LOGGER.error(
					"An error occurred writing new encryption token to file: " + ex.getLocalizedMessage(),
					ex);
			throw ex;
		}
		return success;
	}

	/*
	 * INTERNAL METHODS
	 */
	/**
	 * Returns the path on the file system to the resource for the token
	 * 
	 * @return Path to resource to get the token
	 */
	public String getResourceLocation() {
		return resourceLocation;
	}

	/**
	 * Sets the path to the resource for the token
	 * 
	 * @param resourceLocation
	 *            Path to resource to get the token
	 */
	public void setResourceLocation(
			String resourceLoc )
			throws Throwable {
		resourceLocation = resourceLoc;
	}

	/**
	 * Checks to see if the data is properly wrapped with ENC{}
	 * 
	 * @param data
	 * @return boolean - true if properly wrapped, false otherwise
	 */
	public static boolean isProperlyWrapped(
			String data ) {
		return ENCCodePattern.matcher(
				data).matches();
	}

	/**
	 * Converts a binary value to a encoded string
	 * 
	 * @param data
	 *            Binary value to encode as an encoded string
	 * @return Encoded string from the binary value specified
	 */
	private String toString(
			byte[] data ) {
		return Hex.encodeHexString(data);
	}

	/**
	 * Converts a string value to a decoded binary
	 * 
	 * @param data
	 *            String value to convert to decoded hex
	 * @return Decoded binary from the string value specified
	 */
	private byte[] fromString(
			String data ) {
		try {
			return Hex.decodeHex(data.toCharArray());
		}
		catch (DecoderException e) {
			LOGGER.error(
					e.getLocalizedMessage(),
					e);
			return null;
		}
	}

	/**
	 * Method to generate a new secret key from the specified token key file
	 */
	private void generateRootKeyFromToken()
			throws Throwable {

		if (!tokenFile.exists()) {
			throw new Throwable(
					"Token file not found at specified path [" + getResourceLocation() + "]");
		}
		try {
			String strPassword = FileUtils.readFileContent(tokenFile);
			char[] password = strPassword != null ? strPassword.trim().toCharArray() : null;
			SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
			SecretKey tmp = factory.generateSecret(new PBEKeySpec(
					password,
					salt,
					65536,
					256));
			setKey(new SecretKeySpec(
					tmp.getEncoded(),
					KEY_ENCRYPTION_ALGORITHM));
		}
		catch (Exception ex) {
			LOGGER.error(
					"An error occurred generating the root key from the specified token: " + ex.getLocalizedMessage(),
					ex);
		}
	}

	/**
	 * Method to generate a new random token key value
	 * 
	 * @return
	 * @throws Exception
	 */
	private static String generateRandomSecretKey()
			throws Exception {
		KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
		keyGenerator.init(256);
		SecretKey secretKey = keyGenerator.generateKey();
		byte[] encoded = secretKey.getEncoded();
		return DatatypeConverter.printBase64Binary(encoded);
	}

	/**
	 * Set the key to use
	 * 
	 * @param key
	 */
	protected void setKey(
			Key key ) {
		this.key = key;
	}

	/**
	 * Get the key to use
	 * 
	 * @return
	 */
	protected Key getKey() {
		return this.key;
	}

	/*
	 * ENCRYPTION METHODS
	 */
	/**
	 * Method to encrypt and hex-encode a string value using the specified token
	 * resource
	 * 
	 * @param data
	 *            String to encrypt
	 * @return Encrypted and Hex-encoded string value using the specified token
	 *         resource
	 * @throws Exception
	 */
	public String encryptAndHexEncode(
			String data )
			throws Exception {
		if (data == null) {
			return null;
		}
		byte[] encryptedBytes = encryptBytes(data.getBytes("UTF-8"));
		return PREFIX + toString(encryptedBytes) + SUFFIX;
	}

	/*
	 * DECRYPTION METHODS
	 */
	/**
	 * Returns a decrypted value from the encrypted hex-encoded value specified
	 * 
	 * @param data
	 *            Hex-Encoded string value to decrypt
	 * @return Decrypted value from the encrypted hex-encoded value specified
	 * @throws Exception
	 */
	public String decryptHexEncoded(
			String data )
			throws Exception {
		if (data == null) {
			return null;
		}
		Matcher matcher = ENCCodePattern.matcher(data);
		if (matcher.matches()) {
			String codedString = matcher.group(1);
			return new String(
					decryptBytes(fromString(codedString)),
					"UTF-8");
		}
		else {
			return data;
		}
	}

	/*
	 * ABSTRACT METHODS
	 */
	/**
	 * Encrypt the data as a byte array
	 * 
	 * @param valueToEncrypt
	 *            value to encrypt
	 */
	abstract public byte[] encryptBytes(
			byte[] valueToEncrypt )
			throws Exception;

	/**
	 * Decrypt the encrypted data
	 * 
	 * @param valueToDecrypt
	 *            value to encrypt
	 */
	abstract public byte[] decryptBytes(
			byte[] valueToDecrypt )
			throws Exception;
}