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
package org.locationtech.geowave.adapter.vector.stats;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.geotime.store.statistics.FieldNameStatistic;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.NumericRangeDataStatistics;
import org.opengis.feature.simple.SimpleFeature;

public class FeatureNumericRangeStatistics extends
		NumericRangeDataStatistics<SimpleFeature> implements
		FieldNameStatistic
{

	public static final FieldStatisticsType<Range<Double>> STATS_TYPE = new FieldStatisticsType<>(
			"FEATURE_NUMERIC_RANGE");

	public FeatureNumericRangeStatistics() {
		super();
	}

	public FeatureNumericRangeStatistics(
			final String fieldName ) {
		this(
				null,
				fieldName);
	}

	public FeatureNumericRangeStatistics(
			final Short adapterId,
			final String fieldName ) {
		super(
				adapterId,
				STATS_TYPE,
				fieldName);
	}

	@Override
	public String getFieldName() {
		return extendedId;
	}

	@Override
	protected NumericRange getRange(
			final SimpleFeature entry ) {

		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return null;
		}
		final double num = ((Number) o).doubleValue();
		return new NumericRange(
				num,
				num);
	}

	@Override
	public InternalDataStatistics<SimpleFeature, Range<Double>, FieldStatisticsQueryBuilder<Range<Double>>> duplicate() {
		return new FeatureNumericRangeStatistics(
				adapterId,
				getFieldName());
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"range[internalDataAdapterId=").append(
				super.getAdapterId());
		buffer.append(
				", field=").append(
				getFieldName());
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

		if (!isSet()) {
			return "No Values";
		}
		else {
			final Map<String, Double> map = new HashMap<>();
			map.put(
					"min",
					getMin());
			map.put(
					"max",
					getMax());
			return map;
		}
	}

	/**
	 * Convert Feature Numeric Range statistics to a JSON object
	 */

	public static class FeatureNumericRangeConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 6309383518148391565L;

		@Override
		public InternalDataStatistics<SimpleFeature, Range<Double>, FieldStatisticsQueryBuilder<Range<Double>>> create(
				final Short adapterId,
				final String fieldName ) {
			return new FeatureNumericRangeStatistics(
					adapterId,
					fieldName);
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}
	}
}
