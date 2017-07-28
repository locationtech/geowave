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
package mil.nga.giat.geowave.cli.geoserver;

import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_NAMESPACE_PREFIX;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_PASS;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_URL;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_USER;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_WORKSPACE;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.converters.GeoWaveBaseConverter;
import mil.nga.giat.geowave.core.cli.converters.OptionalPasswordConverter;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;

@GeowaveOperation(name = "geoserver", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for GeoServer")
public class ConfigGeoServerCommand extends
		ServiceEnabledCommand<String>
{
	@Parameter(names = {
		"-u",
		"--username"
	}, description = "GeoServer User")
	private String username;

	// GEOWAVE-811 - adding additional password options for added protection
	@Parameter(names = {
		"-p",
		"--password"
	}, description = "GeoServer Password - " + OptionalPasswordConverter.DEFAULT_PASSWORD_DESCRIPTION, converter = OptionalPasswordConverter.class)
	private String pass;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, description = "GeoServer Default Workspace")
	private String workspace;

	@Parameter(description = "<GeoServer URL>")
	private List<String> parameters = new ArrayList<String>();
	private String url = null;

	@ParametersDelegate
	private GeoServerSSLConfigurationOptions sslConfigOptions = new GeoServerSSLConfigurationOptions();

	@Override
	public boolean prepare(
			final OperationParams params ) {
		boolean retval = true;
		retval |= super.prepare(params);

		final String username = getName();
		final String password = getPass();

		final boolean usernameSpecified = (username != null) && !"".equals(username.trim());
		final boolean passwordSpecified = (password != null) && !"".equals(password.trim());
		if (usernameSpecified || passwordSpecified) {
			if (usernameSpecified && !passwordSpecified) {
				setPass(GeoWaveBaseConverter.promptAndReadPassword("Please enter a password for username [" + username
						+ "]: "));
				if ((getPass() == null) || "".equals(getPass().trim())) {
					throw new ParameterException(
							"Password cannot be null or empty if username is specified");
				}
			}
			else if (passwordSpecified && !usernameSpecified) {
				setName(GeoWaveBaseConverter
						.promptAndReadValue("Please enter a username associated with specified password: "));
				if ((getName() == null) || "".equals(getName().trim())) {
					throw new ParameterException(
							"Username cannot be null or empty if password is specified");
				}
			}
		}

		return retval;
	}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		JCommander.getConsole().println(
				computeResults(params));
	}

	public String getName() {
		return username;
	}

	public void setName(
			final String name ) {
		username = name;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(
			final String pass ) {
		this.pass = pass;
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			final String workspace ) {
		this.workspace = workspace;
	}

	public GeoServerSSLConfigurationOptions getGeoServerSSLConfigurationOptions() {
		return sslConfigOptions;
	}

	public void setGeoServerSSLConfigurationOptions(
			final GeoServerSSLConfigurationOptions sslConfigOptions ) {
		this.sslConfigOptions = sslConfigOptions;
	}

	@Override
	public String usage() {
		StringBuilder builder = new StringBuilder();

		final List<String> nameArray = new ArrayList<String>();
		final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(this);
		final JCommanderTranslationMap map = translator.translate();
		map.createFacadeObjects();

		// Copy default parameters over for help display.
		map.transformToFacade();

		JCommander jc = new JCommander();

		final Map<String, TranslationEntry> translations = map.getEntries();
		for (final Object obj : map.getObjects()) {
			for (final Field field : obj.getClass().getDeclaredFields()) {
				final TranslationEntry tEntry = translations.get(field.getName());
				if ((tEntry != null) && (tEntry.getObject() instanceof ConfigGeoServerCommand)) {
					jc.addObject(obj);
					break;
				}
			}
		}

		final String programName = StringUtils.join(
				nameArray,
				" ");
		jc.setProgramName(programName);
		jc.usage(builder);

		// Trim excess newlines.
		final String operations = builder.toString().trim();

		builder = new StringBuilder();
		builder.append(operations);
		builder.append("\n\n");
		builder.append("  ");

		jc = new JCommander();

		for (final Object obj : map.getObjects()) {
			for (final Field field : obj.getClass().getDeclaredFields()) {
				final TranslationEntry tEntry = translations.get(field.getName());
				if ((tEntry != null) && !(tEntry.getObject() instanceof ConfigGeoServerCommand)) {
					final Parameters parameters = tEntry.getObject().getClass().getAnnotation(
							Parameters.class);
					if (parameters != null) {
						builder.append(parameters.commandDescription());
					}
					else {
						builder.append("Additional Parameters");
					}
					jc.addObject(obj);
					break;
				}
			}
		}

		jc.setProgramName(programName);
		jc.usage(builder);
		builder.append("\n\n");

		return builder.toString().trim();
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws Exception {

		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <GeoServer URL>");
		}
		url = parameters.get(0);
		final Properties existingProps = getGeoWaveConfigProperties(params);

		// all switches are optional
		if (url != null) {
			existingProps.setProperty(
					GEOSERVER_URL,
					url);
		}

		if (getName() != null) {
			existingProps.setProperty(
					GEOSERVER_USER,
					getName());
		}

		if (getPass() != null) {
			existingProps.setProperty(
					GEOSERVER_PASS,
					getPass());
		}

		if (getWorkspace() != null) {
			existingProps.setProperty(
					GEOSERVER_WORKSPACE,
					getWorkspace());
		}

		// save properties from ssl configurations
		sslConfigOptions.saveProperties(existingProps);

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps,
				this.getClass(),
				GEOSERVER_NAMESPACE_PREFIX);
		GeoServerRestClient.invalidateInstance();

		// generate a return for rest calls
		StringBuilder builder = new StringBuilder();
		for (Object key : existingProps.keySet()) {
			if (key.toString().startsWith(
					"geoserver")) {
				builder.append(key.toString() + "=" + existingProps.getProperty(key.toString()) + "\n");
			}
		}
		return builder.toString();
	}
}
