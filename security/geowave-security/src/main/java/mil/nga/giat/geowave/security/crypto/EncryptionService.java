package mil.nga.giat.geowave.security.crypto;

import java.security.Key;

/**
 * An Encryption service represents a dedicated channel to encrypt content on
 * behalf of a specific application.
 * 
 * @author mark.carirer
 */
public interface EncryptionService
{

	/**
	 * Encrypts the provided data.
	 * 
	 * @param data
	 *            data to encrypt
	 * @return An encrypted binary value of the clear data specified
	 * @throws Exception
	 */
	public byte[] encrypt(
			byte[] data )
			throws Exception;

	/**
	 * Decrypts the provided data.
	 * 
	 * @param data
	 *            encrypted data to decrypt
	 * @return A decrypted binary value of the encrypted data specified
	 * @throws Exception
	 */
	public byte[] decrypt(
			byte[] data )
			throws Exception;

	/**
	 * Sets the key to use for encrypting and decrypting the data
	 * 
	 * @param key
	 *            Key to set for this encryption service instance
	 * @throws Exception
	 */
	public void setRootKey(
			Key key )
			throws Exception;
}