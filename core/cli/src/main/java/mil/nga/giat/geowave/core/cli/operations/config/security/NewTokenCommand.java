package mil.nga.giat.geowave.core.cli.operations.config.security;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

@GeowaveOperation(name = "newcryptokey", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Generate a new security cryptography key for use with configuration properties")
public class NewTokenCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger sLog = LoggerFactory.getLogger(NewTokenCommand.class);

	@Override
	public void execute(
			OperationParams params ) {
		sLog.trace("ENTER :: execute");

		File geowaveDir = getGeoWaveDirectory();
		if (geowaveDir != null && geowaveDir.exists()) {
			File tokenFile = getSecurityTokenFile();
			// if token already exists, iterate through config props file and
			// re-encrypt any encrypted values against the new token
			if (tokenFile != null && tokenFile.exists()) {
				try {
					sLog.info("Existing encryption token file exists already at path [" + tokenFile.getCanonicalPath());
					sLog
							.info(
									"Creating new encryption token and migrating all passwords in [{}] to be encrypted with new token",
									ConfigOptions.getDefaultPropertyFile().getCanonicalPath());

					File backupFile = null;
					boolean tokenBackedUp = false;
					try {
						backupFile = new File(
								tokenFile.getCanonicalPath() + ".bak");
						tokenBackedUp = tokenFile.renameTo(backupFile);
						generateNewEncryptionToken(tokenFile);
					}
					catch (Exception ex) {
						sLog
								.error(
										"An error occurred backing up existing token file. Please check directory and permissions and try again.",
										ex);
					}
					if (tokenBackedUp) {
						Properties configProps = getGeoWaveConfigProperties(params);
						if (configProps != null) {
							boolean updated = false;
							Set<Object> keySet = configProps.keySet();
							Iterator<Object> keyIter = keySet.iterator();
							if (keyIter != null) {
								String configKey = null;
								while (keyIter.hasNext()) {
									configKey = (String) keyIter.next();
									String configValue = configProps.getProperty(configKey);
									if (configValue != null && !"".equals(configValue.trim())
											&& BaseEncryption.isProperlyWrapped(configValue)) {
										String decryptedValue = SecurityUtils.decryptHexEncodedValue(
												configValue,
												backupFile.getCanonicalPath());
										String encryptedValue = SecurityUtils.encryptAndHexEncodeValue(
												decryptedValue,
												tokenFile.getCanonicalPath());
										configProps.put(
												configKey,
												encryptedValue);
										updated = true;
									}
								}
							}
							if (updated) {
								ConfigOptions.writeProperties(
										getGeoWaveConfigFile(params),
										configProps);
							}
						}
						backupFile.deleteOnExit();
					}
				}
				catch (Exception ex) {
					sLog.error(
							"An error occurred creating a new encryption token: " + ex.getLocalizedMessage(),
							ex);
				}
			}
			else {
				generateNewEncryptionToken(tokenFile);
			}
		}
		sLog.trace("EXIT :: execute");
	}
}