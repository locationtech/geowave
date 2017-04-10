/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import org.junit.Assert;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

/**
 * Unit test cases for encrypting and decrypting values
 */
public class SecurityUtilsTest
{
	private String resourceLocation;
	private String rawInput;

	/*
	 * public static void main(String[] args) throws Exception { String token1 =
	 * System.getProperty("user.home") + "/.geowave/" +
	 * SecurityUtils.getResourceLocation()+".bak"; String token2 =
	 * System.getProperty("user.home") + "/.geowave/" +
	 * SecurityUtils.getResourceLocation();
	 * 
	 * String rawInput = "geowave"; String encrypted1=new
	 * SecurityUtils().encryptAndHexEncodeValue( rawInput, token1 ); String
	 * decrypted1=new SecurityUtils().decryptHexEncodedValue( encrypted1, token1
	 * );
	 * 
	 * String encrypted2=new SecurityUtils().encryptAndHexEncodeValue( rawInput,
	 * token2 ); String decrypted2=new SecurityUtils().decryptHexEncodedValue(
	 * encrypted2, token2 );
	 * 
	 * System.out.println("\nencrypted1: "+encrypted1);
	 * System.out.println("decrypted1: "+decrypted1);
	 * 
	 * System.out.println("\nencrypted2: "+encrypted2);
	 * System.out.println("decrypted2: "+decrypted2); }
	 */

	public static void main(
			String[] args )
			throws Exception {

		File tokenFile = new File(
				new SecurityUtils().getResourceLocation());
		/*
		 * File backupFile = null; boolean tokenBackedUp = false; try {
		 * backupFile = new File( tokenFile.getCanonicalPath() + ".bak");
		 * tokenBackedUp = tokenFile.renameTo(backupFile);
		 * System.out.println("tokenBackedUp: " + tokenBackedUp);
		 * tokenFile.createNewFile(); } catch (Exception ex) {
		 * ex.printStackTrace(); }
		 */
		// String configValue =
		// "ENC{564a536b784b573837316f4554424b434c676f6c565a48342f516934795062704a4e70435a2f6c7a5141303d}";
		/*
		 * String encryptedValue = new SecurityUtils().encryptAndHexEncodeValue(
		 * "geowave", tokenFile.getCanonicalPath());
		 */
		String encryptedValue = "ENC{564a536b784b573837316f4554424b434c676f6c565a48342f516934795062704a4e70435a2f6c7a5141303d}";
		System.out.println("encryptedValue: " + encryptedValue);

		String decryptedValue = new SecurityUtils().decryptHexEncodedValue(
				encryptedValue,
				tokenFile.getCanonicalPath());
		System.out.println("decryptedValue: " + decryptedValue);
		/*
		 * String decryptedValue = new SecurityUtils().decryptHexEncodedValue(
		 * configValue, backupFile.getCanonicalPath());
		 * System.out.println("decryptedValue: " + decryptedValue);
		 * 
		 * String encryptedValue1 = new SecurityUtils().decryptHexEncodedValue(
		 * configValue, tokenFile.getCanonicalPath());
		 */

	}

	@Before
	public void init() {
		// get the default token path
		resourceLocation = new SecurityUtils().getResourceLocation() + ".bak";
		System.out.println("testing with resource location: " + resourceLocation);
		// test value to use for encrypting and decryping
		rawInput = "geowave";
	}

	/*
	 * @Test public void test1Encryption() throws Exception { String
	 * encryptedValue = new SecurityUtils().encryptAndHexEncodeValue( rawInput,
	 * resourceLocation); System.out.println("encryptedValue: " +
	 * encryptedValue); }
	 */

	@Test
	public void test2Decryption()
			throws Exception {
		String encryptedValue = "ENC{5758675861712b50445a4442626935524f456f4159723169494e6a37724f505a44454f524c657871754a773d}";
		String decrypted = new SecurityUtils().decryptHexEncodedValue(
				encryptedValue,
				resourceLocation);
		System.out.println("decrypted: " + decrypted);

		Assert.assertEquals(
				"Testing equals",
				rawInput,
				decrypted);
	}

	/*
	 * @Test public void testEncryptDecrypt() throws Exception { String
	 * encrypted = new SecurityUtils().encryptAndHexEncodeValue( rawInput,
	 * resourceLocation); System.out.println("encrypted: " + encrypted); String
	 * decrypted = new SecurityUtils().decryptHexEncodedValue( encrypted,
	 * resourceLocation); System.out.println("decrypted: " + decrypted);
	 * 
	 * Assert.assertEquals( "Testing equals", rawInput, decrypted); }
	 */
}