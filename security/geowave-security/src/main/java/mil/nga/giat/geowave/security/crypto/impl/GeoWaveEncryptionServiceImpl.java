package mil.nga.giat.geowave.security.crypto.impl;

import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.security.crypto.EncryptionService;

/**
 * Encryption service implementation for encrypting and decrypting content
 * 
 * @see EncryptionService
 */
public class GeoWaveEncryptionServiceImpl implements
		EncryptionService
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveEncryptionServiceImpl.class);

	private static final String ENCRYPTION_ALGORITHM = "AES/CBC/PKCS5Padding";
	private static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
	private static final int BITS_PER_BYTE = 8;
	private static final int IV_LENGTH = 128 / BITS_PER_BYTE;

	private Key key;
	private static SecureRandom random;

	public GeoWaveEncryptionServiceImpl() {
		init();
	}

	private void init() {
		try {
			setRandom(SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM));
		}
		catch (NoSuchAlgorithmException e) {
			LOGGER.warn(
					"Misconfigured algorithm for SecureRandom",
					e);
			setRandom(new SecureRandom());
		}
	}

	/**
	 * Encrypts a string using the given key and returns a base 64 encoded
	 * encrypted string.
	 * 
	 * @param valueToEnc
	 *            String value to encrypt
	 * @param key
	 *            Security key to encrypt with
	 * @return Encrypted string
	 * @throws Exception
	 */
	public String encrypt(
			String valueToEncrypt )
			throws Exception {
		return Base64.encodeBase64String(encryptValue(valueToEncrypt.getBytes("utf-8")));
	}

	/**
	 * Encrypt the data
	 * 
	 * @param valueToEncrypt
	 *            value to encrypt
	 */
	public byte[] encrypt(
			byte[] valueToEncrypt )
			throws Exception {
		return Base64.encodeBase64(encryptValue(valueToEncrypt));
	}

	/**
	 * Encrypts a binary value using the given key and returns a base 64 encoded
	 * encrypted string.
	 * 
	 * @param valueToEncrypt
	 *            Binary value to encrypt
	 * @return Encrypted binary
	 * @throws Exception
	 */
	public byte[] encryptValue(
			byte[] valueToEncrypt )
			throws Exception {
		LOGGER.trace("ENTER :: encyrpt");
		Cipher c = getCipher(
				Cipher.ENCRYPT_MODE,
				getKey(),
				generateIvParameterSpec());
		byte[] encValue = c.doFinal(valueToEncrypt);
		byte[] finalValue = new byte[c.getIV().length + encValue.length];
		System.arraycopy(
				c.getIV(),
				0,
				finalValue,
				0,
				c.getIV().length);
		System.arraycopy(
				encValue,
				0,
				finalValue,
				c.getIV().length,
				encValue.length);
		return finalValue;
	}

	/**
	 * Decrypt the encrypted data
	 * 
	 * @param valueToDecrypt
	 *            value to encrypt
	 */
	public byte[] decrypt(
			byte[] valueToDecrypt )
			throws Exception {
		LOGGER.trace("ENTER :: decrypt");
		byte[] decodedValue = Base64.decodeBase64(valueToDecrypt);
		byte[] iv = new byte[IV_LENGTH];
		byte[] msg = new byte[decodedValue.length - iv.length];

		System.arraycopy(
				decodedValue,
				0,
				iv,
				0,
				iv.length);
		System.arraycopy(
				decodedValue,
				iv.length,
				msg,
				0,
				msg.length);
		Cipher c = getCipher(
				Cipher.DECRYPT_MODE,
				getKey(),
				iv);
		return c.doFinal(msg);
	}

	/**
	 * Sets the root key to use for generating a key to use for encrypting and
	 * decrypting the data
	 * 
	 * @param rootKey
	 *            Root key to set for this encryption service instance
	 * @throws Exception
	 */
	public void setRootKey(
			Key rootKey ) {
		this.key = rootKey;
	}

	public Key getKey() {
		return key;
	}

	private byte[] generateIvParameterSpec() {
		byte[] iv = new byte[IV_LENGTH];
		random.nextBytes(iv);
		return iv;
	}

	/**
	 * @return the random
	 */
	public static SecureRandom getRandom() {
		return random;
	}

	/**
	 * @param random
	 *            the random to set
	 */
	public static void setRandom(
			SecureRandom random ) {
		GeoWaveEncryptionServiceImpl.random = random;
	}

	/**
	 * Method to generate an MD5 hash of a given string value
	 * 
	 * @param content
	 *            Content to generate the MD5 hash off of
	 * @return String containing the MD5 hash
	 * @throws Exception
	 */
	public static String getMD5Hash(
			byte[] content )
			throws Exception {
		MessageDigest hasher = MessageDigest.getInstance("MD5");
		hasher.update(content);
		byte[] md5Bytes = hasher.digest();
		return convertHashToString(md5Bytes);
	}

	/**
	 * Convert a hash value to a string
	 * 
	 * @param md5Bytes
	 *            MD5 binary to represent as a string
	 * @return String-representation of the specified MD5 hash
	 */
	private static String convertHashToString(
			byte[] md5Bytes ) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < md5Bytes.length; i++) {
			// convert it to a hash value and append
			sb.append(Integer.toString(
					(md5Bytes[i] & 0xff) + 0x100,
					16).substring(
					1));
		}
		return sb.toString();
	}

	/**
	 * Common method for generating a cipher
	 * 
	 * @param mode
	 * @param key
	 * @param iv
	 * @return
	 * @throws Exception
	 */
	private static Cipher getCipher(
			int mode,
			Key key,
			byte[] iv )
			throws Exception {
		Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
		IvParameterSpec ivSpec = new IvParameterSpec(
				iv);
		cipher.init(
				mode,
				key,
				ivSpec);
		return cipher;
	}
}
