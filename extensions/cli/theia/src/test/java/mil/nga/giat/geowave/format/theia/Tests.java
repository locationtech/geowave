/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.theia;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tests
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Tests.class);

	// use the same workspace directory as the ITs to consolidate what is
	// downloaded
	public static final String WORKSPACE_DIR = "../../../test/theia";

	// Default authentication settings filename
	public static final String AUTHENTICATION_FILE = "auth_theia.txt";

	/**
	 * Returns the authentication settings (user/password) to execute tests
	 * 
	 * @return
	 * @throws IOException
	 */
	public static String[] authenticationSettings()
			throws IOException {
		URL authFile = Tests.class.getClassLoader().getResource(
				AUTHENTICATION_FILE);

		BufferedReader inputReader = null;
		InputStream inputStream = null;
		try {
			inputReader = new BufferedReader(
					new InputStreamReader(
							inputStream = authFile.openStream()));
			String line = null;

			while ((line = inputReader.readLine()) != null) {
				return line.split(" ");
			}
		}
		finally {
			if (inputReader != null) {
				IOUtils.closeQuietly(inputReader);
				inputReader = null;
			}
			if (inputStream != null) {
				IOUtils.closeQuietly(inputStream);
				inputStream = null;
			}
		}
		return null;
	}

	/**
	 * Returns whether the authentication file contains valid settings
	 * 
	 * @return
	 * @throws IOException
	 */
	public static boolean authenticationSettingsAreValid()
			throws IOException {
		String[] settings = Tests.authenticationSettings();

		// Did you configure your user/password?
		if (settings == null || settings[0].equals("name.surname@domain.country") || settings[1].equals("password")) {
			LOGGER.warn("You have to register yourself in Theia website to be able to download imagery "
					+ "('https://peps.cnes.fr/'). \n"
					+ "Then you will have to change the credentials in 'auth_theia.txt' file. \n"
					+ "Meanwhile tests which download imagery will be ignored, otherwise they will fail.");

			return false;
		}
		return true;
	}
}
