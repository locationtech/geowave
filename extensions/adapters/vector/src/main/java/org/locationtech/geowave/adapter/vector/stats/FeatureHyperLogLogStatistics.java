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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.geotime.store.statistics.FieldNameStatistic;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * Hyperloglog provides an estimated cardinality of the number of unique values
 * for an attribute.
 *
 *
 */
public class FeatureHyperLogLogStatistics extends
		AbstractDataStatistics<SimpleFeature, HyperLogLogPlus, FieldStatisticsQueryBuilder<HyperLogLogPlus>> implements
		FieldNameStatistic
{
	private final static Logger LOGGER = LoggerFactory.getLogger(FeatureHyperLogLogStatistics.class);
	public static final FieldStatisticsType<HyperLogLogPlus> STATS_TYPE = new FieldStatisticsType<>(
			"ATT_HYPERLLP");

	private HyperLogLogPlus loglog;
	private int precision;

	public FeatureHyperLogLogStatistics() {
		super();
	}

	public FeatureHyperLogLogStatistics(
			final String fieldName,
			final int precision ) {
		this(
				null,
				fieldName,
				precision);
	}

	/**
	 *
	 * @param dataAdapterId
	 * @param fieldName
	 * @param precision
	 *            number of bits to support counting. 2^p is the maximum count
	 *            value per distinct value. 1 <= p <= 32
	 */
	public FeatureHyperLogLogStatistics(
			final Short adapterId,
			final String fieldName,
			final int precision ) {
		super(
				adapterId,
				STATS_TYPE,
				fieldName);
		loglog = new HyperLogLogPlus(
				precision);
		this.precision = precision;
	}

	@Override
	public String getFieldName() {
		return extendedId;
	}

	@Override
	public InternalDataStatistics<SimpleFeature, HyperLogLogPlus, FieldStatisticsQueryBuilder<HyperLogLogPlus>> duplicate() {
		return new FeatureHyperLogLogStatistics(
				adapterId,
				getFieldName(),
				precision);
	}

	public long cardinality() {
		return loglog.cardinality();
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FeatureHyperLogLogStatistics) {
			try {
				loglog = (HyperLogLogPlus) ((FeatureHyperLogLogStatistics) mergeable).loglog.merge(loglog);
			}
			catch (final CardinalityMergeException e) {
				throw new RuntimeException(
						"Unable to merge counters",
						e);
			}
		}

	}

	@Override
	public byte[] toBinary() {

		try {
			final byte[] data = loglog.getBytes();

			final ByteBuffer buffer = super.binaryBuffer(4 + data.length);
			buffer.putInt(data.length);
			buffer.put(data);
			return buffer.array();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception while writing statistic",
					e);
		}
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final byte[] data = new byte[buffer.getInt()];
		buffer.get(data);
		try {
			loglog = HyperLogLogPlus.Builder.build(data);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception while reading statistic",
					e);
		}
	}

	@Override
	public void entryIngested(
			final SimpleFeature entry,
			final GeoWaveRow... rows ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		loglog.offer(o.toString());
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"hyperloglog[internalDataAdapterId=").append(
				super.getAdapterId());
		buffer.append(
				", field=").append(
				getFieldName());
		buffer.append(
				", cardinality=").append(
				loglog.cardinality());
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public HyperLogLogPlus getResult() {
		return loglog;
	}

	@Override
	protected String resultsName() {
		return "hyperloglog";
	}

	@Override
	protected Object resultsValue() {
		final Map<String, Long> result = new HashMap<>();
		result.put(
				"cardinality",
				loglog.cardinality());

		result.put(
				"precision",
				(long) precision);
		return result;
	}

	public static class FeatureHyperLogLogConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 6309383518148391565L;
		private int precision = 16;

		public FeatureHyperLogLogConfig() {

		}

		public FeatureHyperLogLogConfig(
				final int precision ) {
			super();
			this.precision = precision;
		}

		public int getPrecision() {
			return precision;
		}

		public void setPrecision(
				final int precision ) {
			this.precision = precision;
		}

		@Override
		public InternalDataStatistics<SimpleFeature, HyperLogLogPlus, FieldStatisticsQueryBuilder<HyperLogLogPlus>> create(
				final Short internalDataAdapterId,
				final String fieldName ) {
			return new FeatureHyperLogLogStatistics(
					internalDataAdapterId,
					fieldName,
					precision);
		}

		@Override
		public byte[] toBinary() {
			return ByteBuffer.allocate(
					4).putInt(
					precision).array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			precision = ByteBuffer.wrap(
					bytes).getInt();
		}
	}
}
