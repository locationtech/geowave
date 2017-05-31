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
package mil.nga.giat.geowave.adapter.vector.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

public class DateUtilities
{

	public static Date parseISO(
			String input )
			throws java.text.ParseException {

		// NOTE: SimpleDateFormat uses GMT[-+]hh:mm for the TZ which breaks
		// things a bit. Before we go on we have to repair this.
		SimpleDateFormat df = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ssz");

		// this is zero time so we need to add that TZ indicator for
		if (input.endsWith("Z")) {
			input = input.substring(
					0,
					input.length() - 1) + "GMT-00:00";
		}
		else {
			int inset = 6;

			String s0 = input.substring(
					0,
					input.length() - inset);
			String s1 = input.substring(
					input.length() - inset,
					input.length());

			input = s0 + "GMT" + s1;
		}

		return df.parse(input);

	}

	public static TemporalRange getTemporalRange(
			final DataStorePluginOptions dataStorePlugin,
			final ByteArrayId adapterId,
			final String timeField ) {
		final DataStatisticsStore statisticsStore = dataStorePlugin.createDataStatisticsStore();

		// if this is a ranged schema, we have to get complete bounds
		if (timeField.contains("|")) {
			int pipeIndex = timeField.indexOf("|");
			String startField = timeField.substring(
					0,
					pipeIndex);
			String endField = timeField.substring(pipeIndex + 1);

			Date start = null;
			Date end = null;

			ByteArrayId timeStatsId = FeatureTimeRangeStatistics.composeId(startField);

			DataStatistics<?> timeStat = statisticsStore.getDataStatistics(
					adapterId,
					timeStatsId,
					null);
			if (timeStat instanceof FeatureTimeRangeStatistics) {
				final FeatureTimeRangeStatistics trStats = (FeatureTimeRangeStatistics) timeStat;
				start = trStats.asTemporalRange().getStartTime();
			}

			timeStatsId = FeatureTimeRangeStatistics.composeId(endField);

			timeStat = statisticsStore.getDataStatistics(
					adapterId,
					timeStatsId,
					null);
			if (timeStat instanceof FeatureTimeRangeStatistics) {
				final FeatureTimeRangeStatistics trStats = (FeatureTimeRangeStatistics) timeStat;
				end = trStats.asTemporalRange().getEndTime();
			}

			if (start != null && end != null) {
				return new TemporalRange(
						start,
						end);
			}
		}
		else {
			// Look up the time range stat for this adapter
			ByteArrayId timeStatsId = FeatureTimeRangeStatistics.composeId(timeField);

			DataStatistics<?> timeStat = statisticsStore.getDataStatistics(
					adapterId,
					timeStatsId,
					null);
			if (timeStat instanceof FeatureTimeRangeStatistics) {
				final FeatureTimeRangeStatistics trStats = (FeatureTimeRangeStatistics) timeStat;
				return trStats.asTemporalRange();
			}
		}

		return null;
	}
}
