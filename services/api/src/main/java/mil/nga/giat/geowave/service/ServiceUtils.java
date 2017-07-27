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
package mil.nga.giat.geowave.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceUtils.class);

	public static Properties loadProperties(
			final InputStream is ) {
		final Properties props = new Properties();
		if (is != null) {
			try {
				props.load(is);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Could not load properties from InputStream",
						e);
			}
		}
		return props;
	}

	public static String getProperty(
			final Properties props,
			final String name ) {
		if (System.getProperty(name) != null) {
			return System.getProperty(name);
		}
		else if (props.containsKey(name)) {
			return props.getProperty(name);
		}
		else {
			return null;
		}
	}
}
