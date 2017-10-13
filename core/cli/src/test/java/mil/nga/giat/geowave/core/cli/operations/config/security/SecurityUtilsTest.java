/**
 *
 */
package mil.nga.giat.geowave.core.cli.operations.config.security;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

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
		final String rawInput = "geowave";

		final File tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(ConfigOptions.getDefaultPropertyFile());
		if ((tokenFile != null) && tokenFile.exists()) {
			final String encryptedValue = SecurityUtils.encryptAndHexEncodeValue(
					rawInput,
					tokenFile.getCanonicalPath());

			final String decryptedValue = SecurityUtils.decryptHexEncodedValue(
					encryptedValue,
					tokenFile.getCanonicalPath());

			assertEquals(
					decryptedValue,
					rawInput);
		}
	}
}