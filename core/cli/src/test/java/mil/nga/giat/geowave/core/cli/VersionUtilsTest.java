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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class VersionUtilsTest
{

	@Test
	public void testVersion() {
		final String version = null; // change this value when it gives a
										// version
		assertEquals(
				version, // change this value when it gives a version
				VersionUtils.getVersion());
	}
}
