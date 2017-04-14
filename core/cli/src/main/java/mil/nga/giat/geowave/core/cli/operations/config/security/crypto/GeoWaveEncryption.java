/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security.crypto;

import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encryption/Decryption implementation based of symmetric cryptography
 *
 */
public class GeoWaveEncryption extends
		BaseEncryption
{

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveEncryption.class);

	private static final String ENCRYPTION_ALGORITHM = "AES/CBC/PKCS5Padding";
	private static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
	private static final int BITS_PER_BYTE = 8;
	private static final int IV_LENGTH = 128 / BITS_PER_BYTE;

	private static SecureRandom random;

	public GeoWaveEncryption() {
		super();
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
	 * @param random
	 *            the random to set
	 */
	public static void setRandom(
			SecureRandom random ) {
		GeoWaveEncryption.random = random;
	}

	private byte[] generateIvParameterSpec() {
		byte[] iv = new byte[IV_LENGTH];
		random.nextBytes(iv);
		return iv;
	}

	@Override
	public byte[] encryptBytes(
			byte[] valueToEncrypt )
			throws Exception {
		return Base64.encodeBase64(encryptValue(valueToEncrypt));
	}

	@Override
	public byte[] decryptBytes(
			byte[] valueToDecrypt )
			throws Exception {
		return decryptValue(Base64.decodeBase64(valueToDecrypt));
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
		if (key == null) {
			throw new Exception(
					"No key specified");
		}
		Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
		IvParameterSpec ivSpec = new IvParameterSpec(
				iv);
		cipher.init(
				mode,
				key,
				ivSpec);
		return cipher;
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
	private byte[] encryptValue(
			byte[] encodedValue )
			throws Exception {
		LOGGER.trace("ENTER :: encyrpt");
		Cipher c = getCipher(
				Cipher.ENCRYPT_MODE,
				getKey(),
				generateIvParameterSpec());
		byte[] encValue = c.doFinal(encodedValue);
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
	 * Decrypts the base64-decoded value
	 * 
	 * @param decodedValue
	 *            value to decrypt
	 * @return
	 * @throws Exception
	 */
	private byte[] decryptValue(
			byte[] decodedValue )
			throws Exception {
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
}