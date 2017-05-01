/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security;

import java.io.File;

import org.junit.Test;

import static org.junit.Assert.*;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
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

		final String resourceLocation = String.format(
				"%s%s%s",
				System.getProperty("user.home"),
				File.separator,
				ConfigOptions.GEOWAVE_CACHE_PATH);

		final File tokenFile = SecurityUtils.getFormattedTokenKeyFileForParentDir(new File(
				resourceLocation));
		if (tokenFile != null && tokenFile.exists()) {
			String encryptedValue = SecurityUtils.encryptAndHexEncodeValue(
					rawInput,
					tokenFile.getCanonicalPath());
			System.out.println("encryptedValue: " + encryptedValue);

			String decryptedValue = SecurityUtils.decryptHexEncodedValue(
					encryptedValue,
					tokenFile.getCanonicalPath());
			System.out.println("decryptedValue: " + decryptedValue);

			assertEquals(
					decryptedValue,
					rawInput);
		}
	}
}