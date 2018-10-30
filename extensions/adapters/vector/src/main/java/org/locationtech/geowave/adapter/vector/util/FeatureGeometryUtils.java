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
package org.locationtech.geowave.adapter.vector.util;

import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;

import com.vividsolutions.jts.geom.Envelope;

public class FeatureGeometryUtils
{

	public static Envelope getGeoBounds(
			final DataStorePluginOptions dataStorePlugin,
			final String typeName,
			final String geomField ) {
		final DataStatisticsStore statisticsStore = dataStorePlugin.createDataStatisticsStore();
		final InternalAdapterStore internalAdapterStore = dataStorePlugin.createInternalAdapterStore();
		final short adapterId = internalAdapterStore.getAdapterId(typeName);
		final StatisticsQuery<Envelope> query = VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
				geomField).build();
		try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> geoStatIt = statisticsStore.getDataStatistics(
				adapterId,
				query.getExtendedId(),
				query.getStatsType(),
				query.getAuthorizations())) {
			if (geoStatIt.hasNext()) {
				final InternalDataStatistics<?, ?, ?> geoStat = geoStatIt.next();
				if (geoStat != null) {
					if (geoStat instanceof FeatureBoundingBoxStatistics) {
						final FeatureBoundingBoxStatistics bbStats = (FeatureBoundingBoxStatistics) geoStat;
						return new Envelope(
								bbStats.getMinX(),
								bbStats.getMaxX(),
								bbStats.getMinY(),
								bbStats.getMaxY());
					}
				}
			}
		}
		return null;
	}

}
