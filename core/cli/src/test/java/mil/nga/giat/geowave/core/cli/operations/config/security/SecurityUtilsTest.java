/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security;

import java.io.File;

import org.junit.Test;

import static org.junit.Assert.*;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

/**
 * Unit test cases for encrypting and decrypting values
 */
public class SecurityUtilsTest
{
	@Test
	public void testEncryptionDecryption()
			throws Exception {
		String rawInput = "geowave";

		File tokenFile = new File(
				new SecurityUtils().getResourceLocation());
		if (tokenFile != null && tokenFile.exists()) {
			String encryptedValue = new SecurityUtils().encryptAndHexEncodeValue(
					rawInput,
					tokenFile.getCanonicalPath());
			System.out.println("encryptedValue: " + encryptedValue);

			String decryptedValue = new SecurityUtils().decryptHexEncodedValue(
					encryptedValue,
					tokenFile.getCanonicalPath());
			System.out.println("decryptedValue: " + decryptedValue);

			assertEquals(
					decryptedValue,
					rawInput);
		}
	}
}