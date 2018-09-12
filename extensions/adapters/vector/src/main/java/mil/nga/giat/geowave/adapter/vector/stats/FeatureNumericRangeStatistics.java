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
package mil.nga.giat.geowave.adapter.vector.stats;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.NumericRangeDataStatistics;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class FeatureNumericRangeStatistics extends
		NumericRangeDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
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
			final Short internalDataAdapterId,
			final String fieldName ) {
		super(
				internalDataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						fieldName));
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

	@Override
	protected NumericRange getRange(
			final SimpleFeature entry ) {

		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return null;
		}
		final long num = ((Number) o).longValue();
		return new NumericRange(
				num,
				num);
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureNumericRangeStatistics(
				internalDataAdapterId,
				getFieldName());
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"range[internalDataAdapterId=").append(
				super.getInternalDataAdapterId());
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

	/**
	 * Convert Feature Numeric Range statistics to a JSON object
	 */

	@Override
	public JSONObject toJSONObject(
			final InternalAdapterStore store )
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));
		jo.put(
				"field_identifier",
				getFieldName());

		if (!isSet()) {
			jo.put(
					"range",
					"No Values");
		}
		else {
			jo.put(
					"range_min",
					getMin());
			jo.put(
					"range_max",
					getMax());
		}

		return jo;
	}

	public static class FeatureNumericRangeConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 6309383518148391565L;

		@Override
		public DataStatistics<SimpleFeature> create(
				final Short internalDataAdapterId,
				final String fieldName ) {
			return new FeatureNumericRangeStatistics(
					internalDataAdapterId,
					fieldName);
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {}
	}
}
