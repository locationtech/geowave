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