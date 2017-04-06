/**
 * 
 */
package mil.nga.giat.geowave.security.crypto.impl;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.util.Scanner;
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
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.security.crypto.EncryptionService;

/**
 * Cryptography encryption/decryption capability in symmetric mode. This handles
 * the main wrapper functionalities for encrypting and decrypting, though
 * implementation-specifics are handled through an implementation of the
 * EncryptionService interface
 * 
 * @see EncryptionService
 * @see GeoWaveEncryptionServiceImpl
 *
 */
public class GeoWaveEncryptionService
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveEncryptionService.class);

	public String resourceLocation = "geowave_crypto_key.dat";
	private byte[] token = null;
	private Key rootKey = null;

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

	private EncryptionService encryptionService;

	public GeoWaveEncryptionService() {
		try {
			salt = "Ge0W@v3-Ro0t-K3y".getBytes("UTF-8");
			PrefixBytes = PREFIX.getBytes("UTF-8");
			SuffixBytes = SUFFIX.getBytes("UTF-8");
			PrefixBytesLength = PrefixBytes.length;
			SuffixBytesLength = SuffixBytes.length;
		}
		catch (UnsupportedEncodingException e) {
			LOGGER.error(
					e.getLocalizedMessage(),
					e);
		}
		setEncryptionService(new GeoWaveEncryptionServiceImpl());
	}

	/*
	 * INTERNAL METHODS
	 */
	/**
	 * Returns the path to the resource for the token
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
			String resourceLoc ) {
		resourceLocation = resourceLoc;
		generateRootKeyFromToken();
	}

	/**
	 * Returns the encryption service instance for this token encryption class
	 * 
	 * @return the encryption service instance for this token encryption class
	 */
	public EncryptionService getEncryptionService() {
		return encryptionService;
	}

	/**
	 * Sets the encryption service instance for this token encryption class
	 * 
	 * @param encryptionService
	 *            the encryption service instance for this token encryption
	 *            class
	 */
	public void setEncryptionService(
			EncryptionService encryptionService ) {
		this.encryptionService = encryptionService;
	}

	/**
	 * Checks to see if the data is properly wrapped with ENC{}
	 * 
	 * @param data
	 * @return boolean - true if properly wrapped, false otherwise
	 */
	public boolean isProperlyWrapped(
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
	private void generateRootKeyFromToken() {
		try {
			String strPassword = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(
					getResourceLocation()));
			char[] password = strPassword != null ? strPassword.trim().toCharArray() : null;
			SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
			SecretKey tmp = factory.generateSecret(new PBEKeySpec(
					password,
					salt,
					65536,
					256));
			rootKey = new SecretKeySpec(
					tmp.getEncoded(),
					KEY_ENCRYPTION_ALGORITHM);
			getEncryptionService().setRootKey(
					rootKey);
		}
		catch (Exception ex) {
			LOGGER.fatal(
					"An error occurred generating the root key from the specified token: " + ex.getLocalizedMessage(),
					ex);
		}
	}

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

	/**
	 * Sets the resource token to null
	 */
	public synchronized void resetToken() {
		token = null;
	}

	/**
	 * Checks for a token at the specified location
	 */
	private synchronized void checkForToken() {
		if (token == null) {
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream(
					resourceLocation);
			if (stream != null) {
				@SuppressWarnings("resource")
				Scanner s = new Scanner(
						stream,
						"UTF-8").useDelimiter("\\A");
				String content = s.hasNext() ? s.next() : "";
				if (content != null) {
					try {
						token = content.getBytes("UTF-8");
					}
					catch (UnsupportedEncodingException e) {
						LOGGER.error(
								e.getLocalizedMessage(),
								e);
					}
				}
			}
		}
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
		checkForToken();
		byte[] encryptedBytes = getEncryptionService().encrypt(
				data);
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
		checkForToken();
		if (data == null) {
			return null;
		}
		byte[] encryptedBytes = getEncryptionService().encrypt(
				data.getBytes("UTF-8"));
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
		checkForToken();
		if (bytesSurroundedByWrapper(data)) {
			return getEncryptionService().decrypt(
					extractWrappedContents(data));
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
			checkForToken();
			return getEncryptionService().decrypt(
					data);
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
		checkForToken();
		if (data == null) {
			return null;
		}
		Matcher matcher = ENCCodePattern.matcher(data);
		if (matcher.matches()) {
			String codedString = matcher.group(1);
			return new String(
					getEncryptionService().decrypt(
							fromString(codedString)),
					"UTF-8");
		}
		else {
			return data;
		}
	}
}