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
package mil.nga.giat.geowave.core.geotime.store.statistics;

import java.util.Date;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.NumericRangeDataStatistics;

abstract public class TimeRangeDataStatistics<T> extends
		NumericRangeDataStatistics<T>
{
	public final static ByteArrayId STATS_TYPE = new ByteArrayId(
			"TIME_RANGE");

	protected TimeRangeDataStatistics() {
		super();
	}

	public TimeRangeDataStatistics(
			final Short internalAdapterId,
			final String fieldId ) {
		super(
				internalAdapterId,
				composeId(
						STATS_TYPE.getString(),
						fieldId));
	}

	public TemporalRange asTemporalRange() {
		return new TemporalRange(
				new Date(
						(long) this.getMin()),
				new Date(
						(long) this.getMax()));
	}

	/**
	 * Convert Time Range statistics to a JSON object
	 */

	public JSONObject toJSONObject(
			InternalAdapterStore store )
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));
		jo.put(
				"statisticsID",
				getStatisticsId().getString());

		if (!isSet()) {
			jo.put(
					"range",
					"No Values");
		}
		else {
			jo.put(
					"range_min",
					new Date(
							(long) this.getMin()));
			jo.put(
					"range_max",
					new Date(
							(long) this.getMax()));
		}

		return jo;
	}

}
