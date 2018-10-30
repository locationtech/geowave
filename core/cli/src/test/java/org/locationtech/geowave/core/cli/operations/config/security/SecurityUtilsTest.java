/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
/**
 *
 */
package org.locationtech.geowave.core.cli.operations.config.security;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;

/**
 * Unit test cases for encrypting and decrypting values
 */
public class SecurityUtilsTest
{
	@Test
	public void testEncryptionDecryption()
			throws Exception {
		final String rawInput = "geowave";

		final File tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(ConfigOptions.getDefaultPropertyFile());
		if ((tokenFile != null) && tokenFile.exists()) {
			final String encryptedValue = SecurityUtils.encryptAndHexEncodeValue(
					rawInput,
					tokenFile.getCanonicalPath());

			final String decryptedValue = SecurityUtils.decryptHexEncodedValue(
					encryptedValue,
					tokenFile.getCanonicalPath());

			assertEquals(
					decryptedValue,
					rawInput);
		}
	}
}
