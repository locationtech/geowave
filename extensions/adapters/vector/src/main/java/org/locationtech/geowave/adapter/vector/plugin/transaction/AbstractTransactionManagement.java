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
import java.util.Map;

import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
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
	public Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> getDataStatistics() {
		final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats = new HashMap<>();
		final GeotoolsFeatureDataAdapter adapter = components.getAdapter();
		final short internalAdapterId = components.getGTstore().getInternalAdapterStore().getAdapterId(
				adapter.getTypeName());

		try (CloseableIterator<InternalDataStatistics<?, ?, ?>> it = components.getStatsStore().getDataStatistics(
				internalAdapterId,
				composeAuthorizations())) {
			while (it.hasNext()) {
				final InternalDataStatistics<?, ?, ?> stat = it.next();
				stats.put(
						new StatisticsId(
								stat.getType(),
								stat.getExtendedId()),
						(InternalDataStatistics<SimpleFeature, ?, ?>) stat);
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
