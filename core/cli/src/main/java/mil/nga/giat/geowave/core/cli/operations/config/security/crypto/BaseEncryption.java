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

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import mil.nga.giat.geowave.core.cli.utils.FileUtils;

/**
 *
 */
public abstract class BaseEncryption
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseEncryption.class);

	public static String resourceName = "geowave_crypto_key.dat";
	private String resourceLocation;
	private Key key = null;

	/**
	 * PROTECT this value. The salt value is the second-half of the protection
	 * mechanism for key used when encrypting or decrypting the content.<br/>
	 * We cannot generate a new random cryptography key each time, as that would
	 * mean two different keys.<br/>
	 * At the same time, encrypted values would be very vulnerable to
	 * unintentional exposure if (a wrong) someone got access to the token key
	 * file, so this salt allows us to protect the encryption with "2 locks" -
	 * both are needed to decrypt a value that was encrypted with the SAME two
	 * values (salt - below - and token file - specified at resourceLocation)
	 */
	private byte[] salt = null;
	protected static File tokenFile = null;

	public static final String PREFIX = "ENC{";
	public static final String SUFFIX = "}";
	public static final String WRAPPER = PREFIX + SUFFIX;
	private static final Pattern ENCCodePattern = Pattern.compile(PREFIX.replace(
			"{",
			"\\{") + "([^}]+)" + SUFFIX.replace(
			"{",
			"\\{"));
	private byte[] PrefixBytes;
	private byte[] SuffixBytes;
	private int PrefixBytesLength;
	private int SuffixBytesLength;

	private final String KEY_ENCRYPTION_ALGORITHM = "AES";

	public BaseEncryption() {
		try {
			checkForToken();
			setResourceLocation(tokenFile.getCanonicalPath());

			salt = "Ge0W@v3-Ro0t-K3y".getBytes("UTF-8");

			PrefixBytes = PREFIX.getBytes("UTF-8");
			SuffixBytes = SUFFIX.getBytes("UTF-8");
			PrefixBytesLength = PrefixBytes.length;
			SuffixBytesLength = SuffixBytes.length;

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
	public static void checkForToken() throws Throwable {
		tokenFile = new File(
				mil.nga.giat.geowave.core.cli.utils.FileUtils.formatFilePath("~" + File.separator
						+ ConfigOptions.GEOWAVE_CACHE_PATH),
				resourceName);
		if (tokenFile == null || !tokenFile.exists()) {
			generateNewEncryptionToken(tokenFile);
		}
		if (tokenFile==null || !tokenFile.exists()) {
			throw new Throwable("An error occurred generating a new encryption token.");
		}
	}

	/**
	 * Generate a new token value in a specified file
	 * 
	 * @param tokenFile
	 * @return
	 */
	public static boolean generateNewEncryptionToken(
			File tokenFile ) throws Exception {
		boolean success = false;
		try {
			LOGGER.info(
					"Writing new encryption token to file at path {}",
					tokenFile.getCanonicalPath());
			org.apache.commons.io.FileUtils.writeStringToFile(
					tokenFile,
					SecurityUtils.generateNewToken());
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
	 * Returns if the specified data is wrapped with the "ENC{}" wrapper
	 * 
	 * @param data
	 *            Data to check if it is wrapped with the "ENC{}" wrapper
	 * @return boolean specifying if the data is wrapped with the wrapper
	 */
	private boolean bytesSurroundedByWrapper(
			byte[] data ) {
		try {
			for (int i = 0; i < PrefixBytesLength; i++) {
				if (data[i] != PrefixBytes[i]) {
					return false;
				}
			}
			int dataLength = data.length;
			int allButPostfixLength = dataLength - SuffixBytesLength;
			for (int i = 0; i < SuffixBytesLength; i++) {
				if (data[allButPostfixLength + i] != SuffixBytes[i]) {
					return false;
				}
			}
		}
		catch (Exception e) {
			return false;
		}
		return true;
	}

	/**
	 * Extracts the inner-content from the encrypted wrapped contents. This
	 * extracts all content from within the "ENC{}", returning everything within
	 * the braces.
	 * 
	 * @param wrappedContents
	 *            "ENC{}"-wrapped content
	 * @return binary content within the "ENC{}" wrapper.
	 */
	private byte[] extractWrappedContents(
			byte[] wrappedContents ) {
		int justTheContentsLength = wrappedContents.length - PrefixBytesLength - SuffixBytesLength;
		byte[] justTheContents = new byte[justTheContentsLength];
		System.arraycopy(
				wrappedContents,
				PrefixBytesLength,
				justTheContents,
				0,
				justTheContentsLength);
		return justTheContents;
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
	 * 
	 */
	private void generateRootKeyFromToken()
			throws Throwable {
		/*
		 * File tokenFile = new File( getResourceLocation());
		 */
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
	 * 
	 * @return
	 * @throws Exception
	 */
	public static String generateRandomSecretKey()
			throws Exception {
		String retval = "";
		KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
		keyGenerator.init(256);
		SecretKey secretKey = keyGenerator.generateKey();
		byte[] encoded = secretKey.getEncoded();
		retval = DatatypeConverter.printBase64Binary(encoded);
		return retval;
	}

	protected void setKey(
			Key key ) {
		this.key = key;
	}

	protected Key getKey() {
		return this.key;
	}

	/*
	 * ENCRYPTION METHODS
	 */
	/**
	 * Method to encrypt binary value using the specified token resource
	 * 
	 * @param data
	 *            Binary value to encrypt
	 * @return Encrypted binary using the specified token resource
	 * @throws Exception
	 */
	public byte[] encrypt(
			byte[] data )
			throws Exception {
		byte[] encryptedBytes = encryptBytes(data);
		int encryptedBytesLength = encryptedBytes.length;
		byte[] wrappedBytes = new byte[PrefixBytesLength + encryptedBytesLength + SuffixBytesLength];
		System.arraycopy(
				PrefixBytes,
				0,
				wrappedBytes,
				0,
				PrefixBytesLength);
		System.arraycopy(
				encryptedBytes,
				0,
				wrappedBytes,
				PrefixBytesLength,
				encryptedBytesLength);
		System.arraycopy(
				SuffixBytes,
				0,
				wrappedBytes,
				PrefixBytesLength + encryptedBytesLength,
				SuffixBytesLength);
		return wrappedBytes;
	}

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
	 * Method to decrypt binary value using the specified token resource
	 * 
	 * @param data
	 *            Binary value to decrypt
	 * @return Decrypted binary using the specified token resource
	 * @throws Exception
	 */
	public byte[] decrypt(
			byte[] data )
			throws Exception {
		if (bytesSurroundedByWrapper(data)) {
			return decryptBytes(extractWrappedContents(data));
		}
		else {
			return data;
		}
	}

	/**
	 * Method to decrypt binary value using the specified token resource. Also
	 * allows the option to add a validation for making sure the value being
	 * decrypted is wrapped with "ENC{}"
	 * 
	 * @param data
	 *            Binary value to decrypt
	 * @param passthroughWrapperlessData
	 *            boolean specifying if service should ensure that the value
	 *            being decrypted is wrapped with "ENC{}" before decrypting, or
	 *            if false allow anything to be decrypted
	 * @return Decrypted binary using the specified token resource
	 * @throws Exception
	 */
	public byte[] decrypt(
			byte[] data,
			boolean passthroughWrapperlessData )
			throws Exception {
		if (passthroughWrapperlessData) {
			return decrypt(data);
		}
		else {
			return decryptBytes(data);
		}
	}

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