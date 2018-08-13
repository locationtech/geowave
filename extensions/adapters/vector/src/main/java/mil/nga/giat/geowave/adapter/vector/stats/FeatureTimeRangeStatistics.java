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
package mil.nga.giat.geowave.adapter.vector.stats;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.geotime.store.statistics.TimeRangeDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

public class FeatureTimeRangeStatistics extends
		TimeRangeDataStatistics<SimpleFeature> implements
		FeatureStatistic
{

	public FeatureTimeRangeStatistics() {
		super();
	}

	public FeatureTimeRangeStatistics(
			final String fieldName ) {
		this(
				null,
				fieldName);
	}

	public FeatureTimeRangeStatistics(
			final Short internalDataAdapterId,
			final String fieldName ) {
		super(
				internalDataAdapterId,
				fieldName);
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE.getString(),
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	public Date getMaxTime() {
		final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis((long) getMax());
		return c.getTime();
	}

	public Date getMinTime() {
		final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis((long) getMin());
		return c.getTime();
	}

	@Override
	protected NumericRange getRange(
			final SimpleFeature entry ) {

		final Object o = entry.getAttribute(getFieldName());
		final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		if (o == null) {
			return null;
		}
		if (o instanceof Date) {
			c.setTime((Date) o);
		}
		else if (o instanceof Calendar) {
			c.setTime(c.getTime());
		}
		else if (o instanceof Number) {
			c.setTimeInMillis(((Number) o).longValue());
		}
		final long time = c.getTimeInMillis();
		return new NumericRange(
				time,
				time);
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureTimeRangeStatistics(
				internalDataAdapterId,
				getFieldName());
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis((long) getMin());
		final Date min = c.getTime();
		c.setTimeInMillis((long) getMax());
		final Date max = c.getTime();
		buffer.append(
				"range[internalDataAdapterId=").append(
				super.getInternalDataAdapterId());
		buffer.append(
				", field=").append(
				getFieldName());
		if (isSet()) {
			buffer.append(
					", min=").append(
					min);
			buffer.append(
					", max=").append(
					max);

		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}
}
