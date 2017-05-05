package mil.nga.giat.geowave.core.cli.api;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

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

	private File geowaveDirectory = null;
	private File geowaveConfigFile = null;
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

		File parentDir = (getGeoWaveDirectory() != null) ? getGeoWaveDirectory() : new File(
				mil.nga.giat.geowave.core.cli.utils.FileUtils.formatFilePath("~" + File.separator
						+ ConfigOptions.GEOWAVE_CACHE_PATH));
		File tokenFile = SecurityUtils.getFormattedTokenKeyFileForParentDir(parentDir);
		if (tokenFile == null || !tokenFile.exists()) {
			generateNewEncryptionToken(tokenFile);
		}
		setSecurityTokenFile(tokenFile);
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

		setGeoWaveConfigFile((File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT));

		if (getGeoWaveConfigFile(params) == null) {
			// if file does not exist
			setGeoWaveConfigFile(ConfigOptions.getDefaultPropertyFile());
		}

		setGeowaveDirectory(getGeoWaveConfigFile(
				params).getParentFile());
		if (!getGeoWaveDirectory().exists()) {
			try {
				boolean created = getGeoWaveDirectory().mkdir();
				if (!created) {
					sLog.error("An error occurred creating a user '.geowave' in home directory");
				}
			}
			catch (Exception e) {
				sLog.error(
						"An error occurred creating a user '.geowave' in home directory: " + e.getLocalizedMessage(),
						e);
				throw new ParameterException(
						e);
			}
		}

		if (!getGeoWaveConfigFile(
				params).exists()) {
			// config file does not exist, attempt to create it.
			try {
				if (!getGeoWaveConfigFile(
						params).createNewFile()) {
					throw new Exception(
							"Could not create property cache file: " + getGeoWaveConfigFile(params));
				}
			}
			catch (IOException e) {
				sLog.error(
						"Could not create property cache file: " + getGeoWaveConfigFile(params),
						e);
				throw new ParameterException(
						e);
			}
		}
	}

	/**
	 * Generate a new token value in a specified file
	 * 
	 * @param tokenFile
	 * @return
	 */
	protected boolean generateNewEncryptionToken(
			File tokenFile ) {
		try {
			return BaseEncryption.generateNewEncryptionToken(tokenFile);
		}
		catch (Exception ex) {
			sLog.error(
					"An error occurred writing new encryption token to file: " + ex.getLocalizedMessage(),
					ex);
		}
		return false;
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

	/**
	 * @return the geowaveDirectory
	 */
	public File getGeoWaveDirectory() {
		return geowaveDirectory;
	}

	/**
	 * @param geowaveDirectory
	 *            the geowaveDirectory to set
	 */
	private void setGeowaveDirectory(
			File geowaveDirectory ) {
		this.geowaveDirectory = geowaveDirectory;
	}

	/**
	 * @return the geowaveConfigFile
	 */
	public File getGeoWaveConfigFile(
			OperationParams params ) {
		if (getGeoWaveConfigFile() == null) {
			setGeoWaveConfigFile((File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT));
		}
		return getGeoWaveConfigFile();
	}

	public File getGeoWaveConfigFile() {
		return geowaveConfigFile;
	}

	/**
	 * @param geowaveConfigFile
	 *            the geowaveConfigFile to set
	 */
	private void setGeoWaveConfigFile(
			File geowaveConfigFile ) {
		this.geowaveConfigFile = geowaveConfigFile;
	}

	public Properties getGeoWaveConfigProperties(
			OperationParams params,
			String filter ) {
		return ConfigOptions.loadProperties(
				getGeoWaveConfigFile(params),
				null);
	}

	public Properties getGeoWaveConfigProperties(
			OperationParams params ) {
		return getGeoWaveConfigProperties(
				params,
				null);
	}

	public Properties getGeoWaveConfigProperties() {
		return ConfigOptions.loadProperties(
				getGeoWaveConfigFile(),
				null);
	}

	@Override
	public String usage() {
		return null;
	}
}
