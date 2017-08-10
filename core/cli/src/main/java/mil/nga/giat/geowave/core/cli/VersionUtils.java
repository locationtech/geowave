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
package mil.nga.giat.geowave.core.cli;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

public class VersionUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(VersionUtils.class);

	private static final String BUILD_PROPERTIES_FILE_NAME = "build.properties";
	private static final String VERSION_PROPERTY_KEY = "project.version";

	public static Properties getBuildProperties() {

		final Properties props = new Properties();
		try (InputStream stream = VersionUtils.class.getClassLoader().getResourceAsStream(
				BUILD_PROPERTIES_FILE_NAME);) {

			if (stream != null) {
				props.load(stream);
			}

			return props;
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Cannot read GeoWave build properties to show version information",
					e);
			JCommander.getConsole().print(
					"Cannot read GeoWave build properties to show version information: " + e.getMessage());
		}
		return props;
	}

	public static String getVersion() {
		return getBuildProperties().getProperty(
				VERSION_PROPERTY_KEY);
	}

	public static List<String> getVersionInfo() {
		final List<String> buildAndPropertyList = Arrays.asList(getBuildProperties().toString().split(
				","));
		Collections.sort(buildAndPropertyList.subList(
				1,
				buildAndPropertyList.size()));
		return buildAndPropertyList;
	}

	public static String asLineDelimitedString(
			final List<String> value ) {
		final StringBuilder str = new StringBuilder();
		for (final String v : value) {
			str.append(
					v).append(
					'\n');
		}
		return str.toString();
	}

	public static void printVersionInfo() {
		final List<String> buildAndPropertyList = getVersionInfo();
		for (final String str : buildAndPropertyList) {
			JCommander.getConsole().println(
					str);
		}
	}
}
