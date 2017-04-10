/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

/**
 * Abstract security-related commands for ensuring that proper pre-requisites
 * (geowave home directory, security token) are present
 */
public abstract class SecurityCommands implements
		Command
{

	private final static Logger sLog = LoggerFactory.getLogger(SecurityCommands.class);

	private File geowaveDir = null;
	private File geowaveConfigPropsFile = null;
	private File securityTokenFile = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		try {
			checkForGeoWaveDirectory(params);
			checkForToken();
			return true;
		}
		catch (Exception e) {
			sLog.error(
					"Error occurred during preparing phase: " + e.getLocalizedMessage(),
					e);
		}
		return false;
	}

	/**
	 * Check if encryption token exists. If not, create one initially
	 */
	protected void checkForToken() {
		File tokenFile = new File(
				mil.nga.giat.geowave.core.cli.utils.FileUtils.formatFilePath("~" + File.separator
						+ ConfigOptions.GEOWAVE_CACHE_PATH),
				BaseEncryption.resourceName);
		if (tokenFile == null || !tokenFile.exists()) {
			generateNewEncryptionToken(tokenFile);
			setSecurityTokenFile(tokenFile);
		}
	}

	/**
	 * Ensure that a geowave home directory exists at ~/.geowave. This is where
	 * encryption token file will be stored
	 * 
	 * @param params
	 * @throws Exception
	 */
	private void checkForGeoWaveDirectory(
			OperationParams params )
			throws Exception {
		File geowaveDir = null;
		geowaveConfigPropsFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		if (geowaveConfigPropsFile != null && geowaveConfigPropsFile.exists()) {
			// get parent directory of config properties file
			geowaveDir = geowaveConfigPropsFile.getParentFile();
		}

		// check if parent directory is set, if not, create it
		if (geowaveDir == null) {
			geowaveDir = ConfigOptions.getDefaultPropertyPath();
			if (geowaveDir != null && !geowaveDir.exists()) {
				try {
					boolean created = geowaveDir.mkdir();
					if (!created) {
						sLog.error("An error occurred creating a user '.geowave' in home directory");
					}
				}
				catch (Exception e) {
					sLog.error(
							"An error occurred creating a user '.geowave' in home directory: "
									+ e.getLocalizedMessage(),
							e);
				}
			}
		}
	}

	protected File getGeoWaveDirectory() {
		return geowaveDir;
	}

	protected File getGeoWaveConfigFile() {
		return geowaveConfigPropsFile;
	}

	protected Properties getGeoWaveConfigProperties() {
		return ConfigOptions.loadProperties(
				geowaveConfigPropsFile,
				null);
	}

	/**
	 * Generate a new token value in a specified file
	 * 
	 * @param tokenFile
	 * @return
	 */
	protected boolean generateNewEncryptionToken(
			File tokenFile ) {
		boolean success = false;
		try {
			sLog.info(
					"Writing new encryption token to file at path {}",
					tokenFile.getCanonicalPath());
			FileUtils.writeStringToFile(
					tokenFile,
					SecurityUtils.generateNewToken());
			sLog.info("Completed writing new encryption token to file");
			success = true;
		}
		catch (Exception ex) {
			sLog.error(
					"An error occurred writing new encryption token to file: " + ex.getLocalizedMessage(),
					ex);
		}
		return success;
	}

	/**
	 * @return the securityTokenFile
	 */
	public File getSecurityTokenFile() {
		return securityTokenFile;
	}

	/**
	 * @param securityTokenFile
	 *            the securityTokenFile to set
	 */
	public void setSecurityTokenFile(
			File securityTokenFile ) {
		this.securityTokenFile = securityTokenFile;
	}
}