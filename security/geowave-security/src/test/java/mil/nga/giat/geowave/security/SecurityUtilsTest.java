/**
 * 
 */
package mil.nga.giat.geowave.security;

import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;
import mil.nga.giat.geowave.security.utils.SecurityUtils;

/**
 * Unit test cases for encrypting and decrypting values
 */
public class SecurityUtilsTest
{
	private String resourceLocation;
	private String rawInput;

	public static void main(
			String[] args )
			throws Exception {
		String rawInput = "pass1234";
		String key1 = "geowave_crypto_key.dat";
		String key2 = "geowave_crypto_key2.dat";
		// SecurityUtils.setResourceLocation(key1);

		/*
		 * System.out.println("\n---- encrypting with key1 -------"); String
		 * encryptedValueKey1 = SecurityUtils.encryptAndHexEncodeValue(
		 * rawInput); System.out.println("encryptedValueKey1: " +
		 * encryptedValueKey1);
		 * 
		 * System.out.println("\n---- decrypting with key1 -------"); String
		 * decryptedValueKey1
		 * =SecurityUtils.decryptHexEncodedValue(encryptedValueKey1);
		 * System.out.println("decryptedValueKey1: " + decryptedValueKey1);
		 * 
		 * System.out.println("\n---- encrypting with key2 -------"); String
		 * encryptedValueKey2 = SecurityUtils.encryptAndHexEncodeValue(
		 * "password1234", key2); System.out.println("encryptedValueKey2: " +
		 * encryptedValueKey2);
		 * 
		 * System.out.println("\n---- decrypting with key2 -------"); String
		 * decryptedValueKey2
		 * =SecurityUtils.decryptHexEncodedValue(encryptedValueKey2, key2);
		 * System.out.println("decryptedValueKey2: " + decryptedValueKey2);
		 */

		System.out.println("default resource location: " + SecurityUtils.getResourceLocation());
		System.out.println("\n---- encrypting with key1 -------");
		String encryptedValueKey1 = SecurityUtils.encryptAndHexEncodeValue(
				rawInput,
				key1);
		System.out.println("encryptedValueKey1: " + encryptedValueKey1);
		System.out.println("\n---- encrypting with key2 -------");
		String encryptedValueKey2 = SecurityUtils.encryptAndHexEncodeValue(
				rawInput,
				key2);
		System.out.println("encryptedValueKey2: " + encryptedValueKey2);

		System.out.println("\n---- decrypting with key1 -------");
		String decryptedValueKey1a = SecurityUtils.decryptHexEncodedValue(encryptedValueKey1);
		System.out.println("decryptedValueKey1a: " + decryptedValueKey1a);
		String decryptedValueKey1b = SecurityUtils.decryptHexEncodedValue(
				encryptedValueKey1,
				key2);
		System.out.println("decryptedValueKey1b: " + decryptedValueKey1b);
		System.out.println("\n---- decrypting with key2 -------");
		String decryptedValueKey2a = SecurityUtils.decryptHexEncodedValue(
				encryptedValueKey2,
				key2);
		System.out.println("decryptedValueKey2a: " + decryptedValueKey2a);
		String decryptedValueKey2b = SecurityUtils.decryptHexEncodedValue(encryptedValueKey2);
		System.out.println("decryptedValueKey2b: " + decryptedValueKey2b);
	}

	@Before
	public void init() {
		// get the default token path
		resourceLocation = SecurityUtils.getResourceLocation();
		// test value to use for encrypting and decryping
		rawInput = "pass1234";
	}

	@Test
	public void test1Encryption()
			throws Exception {
		String encryptedValue = SecurityUtils.encryptAndHexEncodeValue(
				rawInput,
				resourceLocation);
		System.out.println("encryptedValue: " + encryptedValue);
	}

	@Test
	public void test2Decryption()
			throws Exception {
		String encryptedValue = "ENC{3338574d493769764c6a484a38514544585a564f4b734e4a6f4b7358474d3037642b4846392b2b615855773d}";
		String decrypted = SecurityUtils.decryptHexEncodedValue(
				encryptedValue,
				resourceLocation);
		System.out.println("decrypted: " + decrypted);

		Assert.assertEquals(
				"Testing equals",
				rawInput,
				decrypted);
	}

	@Test
	public void testEncryptDecrypt()
			throws Exception {
		String encrypted = SecurityUtils.encryptAndHexEncodeValue(
				rawInput,
				resourceLocation);
		System.out.println("encrypted: " + encrypted);
		String decrypted = SecurityUtils.decryptHexEncodedValue(
				encrypted,
				resourceLocation);
		System.out.println("decrypted: " + decrypted);

		Assert.assertEquals(
				"Testing equals",
				rawInput,
				decrypted);
	}
}
