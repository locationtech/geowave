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
package org.locationtech.geowave.cli.osm.osmfeature;

import org.junit.Test;
import org.locationtech.geowave.cli.osm.osmfeature.FeatureConfigParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class FeatureConfigParserTest
{

	protected static final String TEST_RESOURCE_DIR = new File(
			"./src/test/data/").getAbsolutePath().toString();
	protected static final String TEST_DATA_CONFIG = TEST_RESOURCE_DIR + "/" + "test_mapping.json";

	@Test
	public void testFeatureConfigParser()
			throws IOException {
		FeatureConfigParser fcp = new FeatureConfigParser();

		try (FileInputStream fis = new FileInputStream(
				new File(
						TEST_DATA_CONFIG))) {
			fcp.parseConfig(fis);
		}

	}

}
