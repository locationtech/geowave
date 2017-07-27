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
package mil.nga.giat.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;

import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.Parameter;
import org.junit.Assert;
import org.junit.Test;

public class GeoWavePluginConfigTest
{

	@Test
	public void test()
			throws GeoWavePluginException,
			URISyntaxException {
		final List<Param> params = GeoWavePluginConfig.getPluginParams(new MemoryStoreFactoryFamily());
		final HashMap<String, Serializable> paramValues = new HashMap<String, Serializable>();
		for (final Param param : params) {
			if (param.getName().equals(
					GeoWavePluginConfig.LOCK_MGT_KEY)) {
				final List<String> options = (List<String>) param.metadata.get(Parameter.OPTIONS);
				assertNotNull(options);
				assertTrue(options.size() > 0);
				paramValues.put(
						param.getName(),
						options.get(0));
			}
			else if (param.getName().equals(
					GeoWavePluginConfig.FEATURE_NAMESPACE_KEY)) {
				paramValues.put(
						param.getName(),
						new URI(
								"http://test/test"));
			}
			else if (param.getName().equals(
					GeoWavePluginConfig.TRANSACTION_BUFFER_SIZE)) {
				paramValues.put(
						param.getName(),
						1000);
			}
			else if (!param.getName().equals(
					GeoWavePluginConfig.AUTH_URL_KEY)) {
				paramValues.put(
						param.getName(),
						(Serializable) (param.getDefaultValue() == null ? "" : param.getDefaultValue()));
			}
		}
		final GeoWavePluginConfig config = new GeoWavePluginConfig(
				new MemoryStoreFactoryFamily(),
				paramValues);
		Assert.assertEquals(
				1000,
				(int) config.getTransactionBufferSize());
		assertNotNull(config.getLockingManagementFactory());
		assertNotNull(config.getLockingManagementFactory().createLockingManager(
				config));

	}

}
