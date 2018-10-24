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

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.opengis.feature.simple.SimpleFeature;
import org.threeten.extra.Interval;

public class FeatureTimeRangeStatistics extends
		TimeRangeDataStatistics<SimpleFeature> implements
		FieldNameStatistic
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
			final Short adapterId,
			final String fieldName ) {
		super(
				adapterId,
				fieldName);
	}

	@Override
	public String getFieldName() {
		return extendedId;
	}

	public Date getMaxTime() {
		final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis(getMax());
		return c.getTime();
	}

	public Date getMinTime() {
		final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis(getMin());
		return c.getTime();
	}

	@Override
	protected Interval getInterval(
			final SimpleFeature entry ) {
		return TimeUtils.getInterval(
				entry,
				getFieldName());
	}

	@Override
	public InternalDataStatistics<SimpleFeature, Interval, FieldStatisticsQueryBuilder<Interval>> duplicate() {
		return new FeatureTimeRangeStatistics(
				adapterId,
				getFieldName());
	}
}
