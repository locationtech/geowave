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
package org.locationtech.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

abstract public class NumericRangeDataStatistics<T> extends
		AbstractDataStatistics<T, Range<Double>, FieldStatisticsQueryBuilder<Range<Double>>>
{

	private double min = Double.MAX_VALUE;
	private double max = -Double.MAX_VALUE;

	protected NumericRangeDataStatistics() {
		super();
	}

	public NumericRangeDataStatistics(
			final Short internalDataAdapterId,
			final StatisticsType<Range<Double>, FieldStatisticsQueryBuilder<Range<Double>>> type,
			final String fieldName ) {
		super(
				internalDataAdapterId,
				type,
				fieldName);
	}

	public boolean isSet() {
		if ((min == Double.MAX_VALUE) && (max == -Double.MAX_VALUE)) {
			return false;
		}
		return true;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getRange() {
		return max - min;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(16);
		buffer.putDouble(min);
		buffer.putDouble(max);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		min = buffer.getDouble();
		max = buffer.getDouble();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		final NumericRange range = getRange(entry);
		if (range != null) {
			min = Math.min(
					min,
					range.getMin());
			max = Math.max(
					max,
					range.getMax());
		}
	}

	abstract protected NumericRange getRange(
			final T entry );

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof NumericRangeDataStatistics)) {
			final NumericRangeDataStatistics<T> stats = (NumericRangeDataStatistics<T>) statistics;
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
			Map<String, Double> map = new HashMap<>();
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
	public Range<Double> getResult() {
		if (isSet()) {
			return Range.between(
					min,
					max);
		}
		return null;
	}
}
