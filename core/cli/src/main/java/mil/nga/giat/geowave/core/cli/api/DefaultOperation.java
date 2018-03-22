/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.cli.api;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import mil.nga.giat.geowave.core.cli.spi.DefaultConfigProviderSpi;

/**
 * The default operation prevents implementors from having to implement the
 * 'prepare' function, if they don't want to.
 */
public abstract class DefaultOperation implements
		Operation
{
	private final static Logger sLog = LoggerFactory.getLogger(DefaultOperation.class);

	private File geowaveDirectory = null;
	private File geowaveConfigFile = null;
	private File securityTokenFile = null;

	@Override
	public boolean prepare(
			final OperationParams params )
			throws ParameterException {
		try {
			checkForGeoWaveDirectory(params);
		}
		catch (final Exception e) {
			throw new ParameterException(
					"Error occurred during preparing phase: " + e.getLocalizedMessage(),
					e);
		}
		return true;
	}

	/**
	 * Check if encryption token exists. If not, create one initially This
	 * method must assume the config file is set and just names the token file
	 * ${configfile}.key
	 */
	private void checkForToken() {
		final File tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(geowaveConfigFile);
		if ((tokenFile == null) || !tokenFile.exists()) {
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
			final OperationParams params )
			throws Exception {

		setGeoWaveConfigFile(getGeoWaveConfigFile(params));

		if (getGeoWaveConfigFile(params) == null) {
			// if file does not exist
			setGeoWaveConfigFile(ConfigOptions.getDefaultPropertyFile());
			setDefaultConfigProperties(params);
		}

		setGeowaveDirectory(getGeoWaveConfigFile(
				params).getParentFile());
		if (!getGeoWaveDirectory().exists()) {
			try {
				final boolean created = getGeoWaveDirectory().mkdir();
				if (!created) {
					sLog.error("An error occurred creating a user '.geowave' in home directory");
				}
			}
			catch (final Exception e) {
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
			catch (final IOException e) {
				sLog.error(
						"Could not create property cache file: " + getGeoWaveConfigFile(params),
						e);
				throw new ParameterException(
						e);
			}
			setDefaultConfigProperties(params);
		}

		checkForToken();
	}

	/**
	 * Generate a new token value in a specified file
	 *
	 * @param tokenFile
	 * @return
	 */
	protected boolean generateNewEncryptionToken(
			final File tokenFile ) {
		try {
			return BaseEncryption.generateNewEncryptionToken(tokenFile);
		}
		catch (final Exception ex) {
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
			final File securityTokenFile ) {
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
			final File geowaveDirectory ) {
		this.geowaveDirectory = geowaveDirectory;
	}

	/**
	 * @return the geowaveConfigFile
	 */
	public File getGeoWaveConfigFile(
			final OperationParams params ) {
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
			final File geowaveConfigFile ) {
		this.geowaveConfigFile = geowaveConfigFile;
	}

	public Properties getGeoWaveConfigProperties(
			final OperationParams params,
			final String filter ) {
		return ConfigOptions.loadProperties(
				getGeoWaveConfigFile(params),
				filter);
	}

	public Properties getGeoWaveConfigProperties(
			final OperationParams params ) {
		return getGeoWaveConfigProperties(
				params,
				null);
	}

	public Properties getGeoWaveConfigProperties() {
		return ConfigOptions.loadProperties(getGeoWaveConfigFile());
	}

	/**
	 * Uses SPI to find all projects that have defaults to add to the
	 * config-properties file
	 */
	private void setDefaultConfigProperties(
			final OperationParams params ) {
		final Properties defaultProperties = new Properties();
		final Iterator<DefaultConfigProviderSpi> defaultPropertiesProviders = ServiceLoader.load(
				DefaultConfigProviderSpi.class).iterator();
		while (defaultPropertiesProviders.hasNext()) {
			final DefaultConfigProviderSpi defaultPropertiesProvider = defaultPropertiesProviders.next();
			defaultProperties.putAll(defaultPropertiesProvider.getDefaultConfig());
		}
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(),
				defaultProperties);
	}

	@Override
	public String usage() {
		return null;
	}
}
