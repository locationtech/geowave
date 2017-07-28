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
package mil.nga.giat.geowave.core.ingest.spi;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import mil.nga.giat.geowave.core.index.SPIServiceRegistry;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;

public class IngestFormatPluginRegistry
{

	private static Map<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderRegistry = null;

	private IngestFormatPluginRegistry() {}

	@SuppressWarnings("rawtypes")
	private static void initPluginProviderRegistry() {
		pluginProviderRegistry = new HashMap<String, IngestFormatPluginProviderSpi<?, ?>>();
		final Iterator<IngestFormatPluginProviderSpi> pluginProviders = new SPIServiceRegistry(
				IngestFormatPluginRegistry.class).load(IngestFormatPluginProviderSpi.class);
		while (pluginProviders.hasNext()) {
			final IngestFormatPluginProviderSpi pluginProvider = pluginProviders.next();
			pluginProviderRegistry.put(
					ConfigUtils.cleanOptionName(pluginProvider.getIngestFormatName()),
					pluginProvider);
		}
	}

	public static Map<String, IngestFormatPluginProviderSpi<?, ?>> getPluginProviderRegistry() {
		if (pluginProviderRegistry == null) {
			initPluginProviderRegistry();
		}
		return pluginProviderRegistry;
	}
}
