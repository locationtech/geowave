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
package mil.nga.giat.geowave.format.sentinel2;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.geotools.coverageio.gdal.jp2ecw.JP2ECWReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;

public class Tests
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Tests.class);

	// use the same workspace directory as the ITs to consolidate what is
	// downloaded
	public static final String WORKSPACE_DIR = "../../../test/sentinel2";

	// Default authentication settings filename for Theia
	public static final String THEIA_AUTHENTICATION_FILE = "auth_theia.txt";

	// Raster to validate JP2 support in GDAL.
	private final static String JP2_TEST_FILE = "../../../test/data/raster/sentinel2_band_example.jp2";
	// Flag to indicate whether the native JP2ECW plugin is properly setup.
	private static int JP2ECW_PLUGIN_AVAILABLE_FLAG = 0;

	/**
	 * Returns the authentication settings (user/password) to execute tests
	 * 
	 * @return
	 * @throws IOException
	 */
	public static String[] authenticationSettings(
			String providerName )
			throws IOException {
		String resourceName;
		if (providerName.toUpperCase() == "THEIA")
			resourceName = THEIA_AUTHENTICATION_FILE;
		else
			return new String[] {
				"",
				""
			};

		URL authFile = Tests.class.getClassLoader().getResource(
				resourceName);

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
	public static boolean authenticationSettingsAreValid(
			String providerName )
			throws IOException {
		String[] settings = Tests.authenticationSettings(providerName);
		providerName = providerName.toUpperCase();

		// Did you configure your user/password?
		if (providerName == "THEIA"
				&& (settings == null || settings[0].equals("name.surname@domain.country") || settings[1]
						.equals("password"))) {
			LOGGER.warn("You have to register yourself in Theia website to be able to download imagery "
					+ "('https://peps.cnes.fr/'). \n"
					+ "Then you will have to change the credentials in 'auth_theia.txt' file. \n"
					+ "Meanwhile tests which download imagery will be ignored, otherwise they will fail.");

			return false;
		}
		return true;
	}

	/**
	 * Returns a valid time-period for testing a Sentinel2 provider
	 * 
	 * @return
	 * @throws ParseException
	 */
	public static Date[] timePeriodSettings(
			String providerName )
			throws ParseException {
		providerName = providerName.toUpperCase();

		if (providerName == "THEIA") {
			final Date startDate = DateUtilities.parseISO("2018-01-01T00:00:00Z");
			final Date endDate = DateUtilities.parseISO("2018-01-02T00:00:00Z");
			return new Date[] {
				startDate,
				endDate
			};
		}
		if (providerName == "AWS") {
			final Date startDate = DateUtilities.parseISO("2018-01-04T00:00:00Z");
			final Date endDate = DateUtilities.parseISO("2018-01-05T00:00:00Z");
			return new Date[] {
				startDate,
				endDate
			};
		}
		throw new RuntimeException(
				"No valid time-period defined for '" + providerName + "' Sentinel2 provider");
	}

	/**
	 * Returns whether the JP2ECW plugin for GDAL is really working.
	 */
	public static boolean jp2ecwPluginIsWorking() {
		synchronized (Tests.LOGGER) {
			if (JP2ECW_PLUGIN_AVAILABLE_FLAG == 0) {
				System.err.println("Testing whether the JP2ECW plugin for GDAL is really working...");

				try {
					final File file = new File(
							JP2_TEST_FILE);
					final JP2ECWReader reader = new JP2ECWReader(
							file);
					reader.read(null);
					reader.dispose();

					System.err.println("JP2ECW plugin is working!");
					JP2ECW_PLUGIN_AVAILABLE_FLAG = 1;
				}
				catch (final Throwable e) {
					System.err.println("JP2ECW plugin fails, Error='" + e.getMessage() + "'");
					JP2ECW_PLUGIN_AVAILABLE_FLAG = 2;
				}
			}
		}
		return JP2ECW_PLUGIN_AVAILABLE_FLAG == 1;
	}
}
