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
package org.locationtech.geowave.test.basic;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.test.TestUtils;

public abstract class AbstractGeoWaveIT
{
	abstract protected DataStorePluginOptions getDataStorePluginOptions();

	@Before
	public void cleanBefore()
			throws IOException {
		TestUtils.deleteAll(getDataStorePluginOptions());
	}

	@After
	public void cleanAfter()
			throws IOException {
		TestUtils.deleteAll(getDataStorePluginOptions());
	}
}
