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
package org.locationtech.geowave.core.geotime.store.statistics;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.threeten.extra.Interval;

abstract public class TimeRangeDataStatistics<T> extends
		AbstractDataStatistics<T, Interval, FieldStatisticsQueryBuilder<Interval>>
{

	public final static FieldStatisticsType<Interval> STATS_TYPE = new FieldStatisticsType<>(
			"TIME_RANGE");
	private long min = Long.MAX_VALUE;
	private long max = Long.MIN_VALUE;

	protected TimeRangeDataStatistics() {
		super();
	}

	public TimeRangeDataStatistics(
			final Short internalDataAdapterId,
			final String fieldName ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				fieldName);
	}

	public boolean isSet() {
		if ((min == Long.MAX_VALUE) && (max == Long.MIN_VALUE)) {
			return false;
		}
		return true;
	}

	public TemporalRange asTemporalRange() {
		return new TemporalRange(
				new Date(
						getMin()),
				new Date(
						getMax()));
	}

	public long getMin() {
		return min;
	}

	public long getMax() {
		return max;
	}

	public long getRange() {
		return max - min;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(16);
		buffer.putLong(min);
		buffer.putLong(max);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		min = buffer.getLong();
		max = buffer.getLong();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		final Interval range = getInterval(entry);
		if (range != null) {
			min = Math.min(
					min,
					range.getStart().toEpochMilli());
			max = Math.max(
					max,
					range.getEnd().toEpochMilli());
		}
	}

	abstract protected Interval getInterval(
			final T entry );

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof TimeRangeDataStatistics)) {
			final TimeRangeDataStatistics<T> stats = (TimeRangeDataStatistics<T>) statistics;
			if (stats.isSet()) {
				min = Math.min(
						min,
						stats.getMin());
				max = Math.max(
						max,
						stats.getMax());
			}
		}
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"range[adapterId=").append(
				super.getAdapterId());
		if (isSet()) {
			buffer.append(
					", min=").append(
					getMin());
			buffer.append(
					", max=").append(
					getMax());
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	protected String resultsName() {
		return "range";
	}

	@Override
	protected Object resultsValue() {
		if (isSet()) {
			final Map<String, Long> map = new HashMap<>();
			map.put(
					"min",
					min);
			map.put(
					"max",
					max);
			return map;
		}
		else {
			return "undefined";
		}
	}

	@Override
	public Interval getResult() {
		if (isSet()) {
			return Interval.of(
					Instant.ofEpochMilli(min),
					Instant.ofEpochMilli(max));
		}
		return null;
	}
}
