package mil.nga.giat.geowave.core.cli.api;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

/**
 * The default operation prevents implementors from having to implement the
 * 'prepare' function, if they don't want to.
 */
public class DefaultOperation implements
		Operation
{
	private final static Logger sLog = LoggerFactory.getLogger(DefaultOperation.class);

	private File geowaveDir = null;
	private File geowaveConfigPropsFile = null;
	private File securityTokenFile = null;

	public boolean prepare(
			OperationParams params )
			throws ParameterException {
		try {
			checkForGeoWaveDirectory(params);
			checkForToken();
		}
		catch (Exception e) {
			throw new ParameterException(
					"Error occurred during preparing phase: " + e.getLocalizedMessage(),
					e);
		}
		return true;
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
	 * encryption token file will be stored. This method will attempt to load
	 * the config options from the given config file. If it can't find it, it
	 * will try to create it. It will then set the contextual variables
	 * 'properties' and 'properties-file', which can be used by commands to
	 * overwrite/update the properties.
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

		if (geowaveConfigPropsFile == null) {
			geowaveConfigPropsFile = ConfigOptions.getDefaultPropertyFile();
		}

		if (!geowaveConfigPropsFile.exists()) {
			// Attempt to create it.
			try {
				if (!geowaveConfigPropsFile.createNewFile()) {
					throw new Exception(
							"Could not create property cache file: " + geowaveConfigPropsFile);
				}
			}
			catch (IOException e) {
				sLog.error(
						"Could not create property cache file: " + geowaveConfigPropsFile,
						e);
				throw e;
			}
		}

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
					throw e;
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

	protected Properties getGeoWaveConfigProperties(
			String filter ) {
		return ConfigOptions.loadProperties(
				geowaveConfigPropsFile,
				filter);
	}

	protected Properties getGeoWaveConfigProperties() {
		return getGeoWaveConfigProperties(null);
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
