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

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.geotime.store.statistics.FieldNameStatistic;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.opengis.feature.simple.SimpleFeature;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;

/**
 *
 * Maintains an estimate of how may of each attribute value occurs in a set of
 * data.
 *
 * Default values:
 *
 * Error factor of 0.001 with probability 0.98 of retrieving a correct estimate.
 * The Algorithm does not under-state the estimate.
 *
 */
public class FeatureCountMinSketchStatistics extends
		AbstractDataStatistics<SimpleFeature, CountMinSketch, FieldStatisticsQueryBuilder<CountMinSketch>> implements
		FieldNameStatistic
{
	public static final FieldStatisticsType<CountMinSketch> STATS_TYPE = new FieldStatisticsType<>(
			"ATT_SKETCH");
	private CountMinSketch sketch = null;

	public FeatureCountMinSketchStatistics() {
		super();
		sketch = new CountMinSketch(
				0.001,
				0.98,
				7364181);
	}

	public FeatureCountMinSketchStatistics(
			final Short adapterId,
			final String fieldName ) {
		super(
				adapterId,
				STATS_TYPE,
				fieldName);
		sketch = new CountMinSketch(
				0.001,
				0.98,
				7364181);
	}

	public FeatureCountMinSketchStatistics(
			final Short adapterId,
			final String fieldName,
			final double errorFactor,
			final double probabilityOfCorrectness ) {
		super(
				adapterId,
				STATS_TYPE,
				fieldName);
		sketch = new CountMinSketch(
				errorFactor,
				probabilityOfCorrectness,
				7364181);
	}

	@Override
	public String getFieldName() {
		return extendedId;
	}

	@Override
	public InternalDataStatistics<SimpleFeature, CountMinSketch, FieldStatisticsQueryBuilder<CountMinSketch>> duplicate() {
		return new FeatureCountMinSketchStatistics(
				adapterId,
				getFieldName());
	}

	public long totalSampleSize() {
		return sketch.size();
	}

	public long count(
			final String item ) {
		return sketch.estimateCount(item);
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FeatureCountMinSketchStatistics) {
			try {
				sketch = CountMinSketch.merge(
						sketch,
						((FeatureCountMinSketchStatistics) mergeable).sketch);
			}
			catch (final FrequencyMergeException e) {
				throw new RuntimeException(
						"Unable to merge sketches",
						e);
			}
		}

	}

	@Override
	public byte[] toBinary() {
		final byte[] data = CountMinSketch.serialize(sketch);
		final ByteBuffer buffer = super.binaryBuffer(4 + data.length);
		buffer.putInt(data.length);
		buffer.put(data);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final byte[] data = new byte[buffer.getInt()];
		buffer.get(data);
		sketch = CountMinSketch.deserialize(data);
	}

	@Override
	public void entryIngested(
			final SimpleFeature entry,
			final GeoWaveRow... rows ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		sketch.add(
				o.toString(),
				1);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"sketch[adapterId=").append(
				super.getAdapterId());
		buffer.append(
				", field=").append(
				getFieldName());
		buffer.append(
				", size=").append(
				sketch.size());
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public CountMinSketch getResult() {
		return sketch;
	}

	@Override
	protected String resultsName() {
		return "size";
	}

	@Override
	protected Object resultsValue() {
		return sketch.size();
	}

	public static class FeatureCountMinSketchConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 6309383518148391565L;
		private double errorFactor;
		private double probabilityOfCorrectness;

		public FeatureCountMinSketchConfig() {

		}

		public FeatureCountMinSketchConfig(
				final double errorFactor,
				final double probabilityOfCorrectness ) {
			super();
			this.errorFactor = errorFactor;
			this.probabilityOfCorrectness = probabilityOfCorrectness;
		}

		public void setErrorFactor(
				final double errorFactor ) {
			this.errorFactor = errorFactor;
		}

		public void setProbabilityOfCorrectness(
				final double probabilityOfCorrectness ) {
			this.probabilityOfCorrectness = probabilityOfCorrectness;
		}

		public double getErrorFactor() {
			return errorFactor;
		}

		public double getProbabilityOfCorrectness() {
			return probabilityOfCorrectness;
		}

		@Override
		public InternalDataStatistics<SimpleFeature, CountMinSketch, FieldStatisticsQueryBuilder<CountMinSketch>> create(
				final Short internalDataAdapterId,
				final String fieldName ) {
			return new FeatureCountMinSketchStatistics(
					internalDataAdapterId,
					fieldName,
					errorFactor,
					probabilityOfCorrectness);
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buf = ByteBuffer.allocate(16);
			buf.putDouble(errorFactor);
			buf.putDouble(probabilityOfCorrectness);
			return buf.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			errorFactor = buf.getDouble();
			probabilityOfCorrectness = buf.getDouble();
		}
	}
}
