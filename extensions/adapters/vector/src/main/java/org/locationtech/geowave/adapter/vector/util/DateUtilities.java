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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureTimeRangeStatistics;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.threeten.extra.Interval;

public class DateUtilities
{

	public static Date parseISO(
			String input )
			throws java.text.ParseException {

		// NOTE: SimpleDateFormat uses GMT[-+]hh:mm for the TZ which breaks
		// things a bit. Before we go on we have to repair this.
		final SimpleDateFormat df = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ssz");

		// this is zero time so we need to add that TZ indicator for
		if (input.endsWith("Z")) {
			input = input.substring(
					0,
					input.length() - 1) + "GMT-00:00";
		}
		else {
			final int inset = 6;

			final String s0 = input.substring(
					0,
					input.length() - inset);
			final String s1 = input.substring(
					input.length() - inset,
					input.length());

			input = s0 + "GMT" + s1;
		}

		return df.parse(input);

	}

	public static TemporalRange getTemporalRange(
			final DataStorePluginOptions dataStorePlugin,
			final String typeName,
			final String timeField ) {
		final DataStatisticsStore statisticsStore = dataStorePlugin.createDataStatisticsStore();
		final InternalAdapterStore internalAdapterStore = dataStorePlugin.createInternalAdapterStore();
		final short adapterId = internalAdapterStore.getAdapterId(typeName);
		// if this is a ranged schema, we have to get complete bounds
		if (timeField.contains("|")) {
			final int pipeIndex = timeField.indexOf("|");
			final String startField = timeField.substring(
					0,
					pipeIndex);
			final String endField = timeField.substring(pipeIndex + 1);

			Date start = null;
			Date end = null;

			StatisticsQuery<Interval> query = VectorStatisticsQueryBuilder
					.newBuilder()
					.factory()
					.timeRange()
					.fieldName(
							startField)
					.build();
			try (CloseableIterator<InternalDataStatistics<?, ?, ?>> timeStatIt = statisticsStore.getDataStatistics(
					adapterId,
					query.getExtendedId(),
					query.getStatsType(),
					new String[0])) {
				if (timeStatIt.hasNext()) {
					final InternalDataStatistics<?, ?, ?> timeStat = timeStatIt.next();
					if (timeStat instanceof FeatureTimeRangeStatistics) {
						final FeatureTimeRangeStatistics trStats = (FeatureTimeRangeStatistics) timeStat;
						start = trStats.getMinTime();
					}
				}
			}
			query = VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
					endField).build();
			try (CloseableIterator<InternalDataStatistics<?, ?, ?>> timeStatIt = statisticsStore.getDataStatistics(
					adapterId,
					query.getExtendedId(),
					query.getStatsType(),
					new String[0])) {
				if (timeStatIt.hasNext()) {
					final InternalDataStatistics<?, ?, ?> timeStat = timeStatIt.next();
					if (timeStat instanceof FeatureTimeRangeStatistics) {
						final FeatureTimeRangeStatistics trStats = (FeatureTimeRangeStatistics) timeStat;
						end = trStats.getMinTime();
					}
				}
			}

			if ((start != null) && (end != null)) {
				return new TemporalRange(
						start,
						end);
			}
		}
		else {
			// Look up the time range stat for this adapter

			final StatisticsQuery<Interval> query = VectorStatisticsQueryBuilder
					.newBuilder()
					.factory()
					.timeRange()
					.fieldName(
							timeField)
					.build();
			try (CloseableIterator<InternalDataStatistics<?, ?, ?>> timeStatIt = statisticsStore.getDataStatistics(
					adapterId,
					query.getExtendedId(),
					query.getStatsType(),
					new String[0])) {
				if (timeStatIt.hasNext()) {
					final InternalDataStatistics<?, ?, ?> timeStat = timeStatIt.next();
					if (timeStat instanceof FeatureTimeRangeStatistics) {
						final FeatureTimeRangeStatistics trStats = (FeatureTimeRangeStatistics) timeStat;
						return trStats.asTemporalRange();
					}
				}
			}
		}

		return null;
	}
}
