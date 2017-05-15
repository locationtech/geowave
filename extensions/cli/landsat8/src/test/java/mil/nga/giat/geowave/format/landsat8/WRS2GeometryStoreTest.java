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
package mil.nga.giat.geowave.format.landsat8;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;

import org.junit.Test;

import static org.hamcrest.core.IsNull.notNullValue;

public class WRS2GeometryStoreTest
{
	@Test
	public void testGetGeometry()
			throws MalformedURLException,
			IOException {
		WRS2GeometryStore geometryStore = new WRS2GeometryStore(
				Tests.WORKSPACE_DIR);
		assertThat(
				geometryStore.getGeometry(
						1,
						1),
				notNullValue());
	}
}
