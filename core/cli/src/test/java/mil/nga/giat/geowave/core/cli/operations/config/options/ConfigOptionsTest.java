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
package mil.nga.giat.geowave.core.cli.operations.config.options;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

public class ConfigOptionsTest
{
	@Test
	public void testWriteProperty() {
		String parent = String.format(
				"%s",
				System.getProperty("user.home"));
		File path = new File(
				parent);
		File configfile = ConfigOptions.formatConfigFile(
				"0",
				path);
		Properties prop = new Properties();
		String key = "key";
		String value = "value";
		prop.setProperty(
				key,
				value);
		boolean success = ConfigOptions.writeProperties(
				configfile,
				prop);
		if (success) {
			Properties loadprop = ConfigOptions.loadProperties(configfile);
			assertEquals(
					value,
					loadprop.getProperty(key));
		}

	}

}
