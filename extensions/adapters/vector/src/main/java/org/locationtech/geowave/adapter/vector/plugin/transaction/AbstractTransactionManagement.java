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
package org.locationtech.geowave.adapter.vector.plugin.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.opengis.feature.simple.SimpleFeature;

public abstract class AbstractTransactionManagement implements
		GeoWaveTransaction
{

	protected final GeoWaveDataStoreComponents components;

	public AbstractTransactionManagement(
			final GeoWaveDataStoreComponents components ) {
		super();
		this.components = components;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<ByteArrayId, InternalDataStatistics<SimpleFeature>> getDataStatistics() {
		final Map<ByteArrayId, InternalDataStatistics<SimpleFeature>> stats = new HashMap<ByteArrayId, InternalDataStatistics<SimpleFeature>>();
		final GeotoolsFeatureDataAdapter adapter = components.getAdapter();
		short internalAdapterId = components.getGTstore().getInternalAdapterStore().getInternalAdapterId(
				adapter.getAdapterId());

		try (CloseableIterator<InternalDataStatistics<?>> it = components.getStatsStore().getDataStatistics(
				internalAdapterId,
				composeAuthorizations())) {
			while (it.hasNext()) {
				final InternalDataStatistics<?> stat = it.next();
				stats.put(
						stat.getStatisticsType(),
						(InternalDataStatistics<SimpleFeature>) stat);
			}

		}
		catch (final Exception e) {
			GeoWaveTransactionManagement.LOGGER.error(
					"Failed to access statistics from data store",
					e);
		}
		return stats;
	}

}
