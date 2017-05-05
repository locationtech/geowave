/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security.crypto;

import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
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

	/**
	 * Base constructor for encryption, allowing a resource location for the
	 * cryptography token key to be specified, rather than using the
	 * default-generated path
	 * 
	 * @param resourceLocation
	 *            Path to cryptography token key file
	 */
	public GeoWaveEncryption(
			final String resourceLocation ) {
		super(
				resourceLocation);
	}

	/**
	 * Base constructor for encryption
	 */
	public GeoWaveEncryption() {
		super();
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

	private PaddedBufferedBlockCipher getCipher(
			boolean encrypt ) {
		PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(
				new CBCBlockCipher(
						new AESEngine()),
				new PKCS7Padding());
		CipherParameters ivAndKey = new ParametersWithIV(
				new KeyParameter(
						getKey().getEncoded()),
				salt);
		cipher.init(
				encrypt,
				ivAndKey);
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

		PaddedBufferedBlockCipher cipher = getCipher(true);
		byte output[] = new byte[cipher.getOutputSize(encodedValue.length)];
		int length = cipher.processBytes(
				encodedValue,
				0,
				encodedValue.length,
				output,
				0);
		try {
			cipher.doFinal(
					output,
					length);
		}
		catch (CryptoException e) {
			LOGGER.error(
					"An error occurred performing encryption: " + e.getLocalizedMessage(),
					e);
		}
		return output;
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

		StringBuffer result = new StringBuffer();

		PaddedBufferedBlockCipher cipher = getCipher(false);
		byte output[] = new byte[cipher.getOutputSize(decodedValue.length)];
		int length = cipher.processBytes(
				decodedValue,
				0,
				decodedValue.length,
				output,
				0);
		cipher.doFinal(
				output,
				length);
		if (output != null && output.length != 0) {
			String retval = new String(
					output,
					"UTF-8");
			for (int i = 0; i < retval.length(); i++) {
				char c = retval.charAt(i);
				if (c != 0) {
					result.append(c);
				}
			}
		}
		return result.toString().getBytes(
				"UTF-8");
	}
}
