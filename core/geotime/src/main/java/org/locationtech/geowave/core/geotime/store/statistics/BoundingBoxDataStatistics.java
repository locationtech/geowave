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
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintData;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintSet;

import com.vividsolutions.jts.geom.Envelope;

abstract public class BoundingBoxDataStatistics<T> extends
		AbstractDataStatistics<T, Envelope, FieldStatisticsQueryBuilder<Envelope>>
{
	public final static FieldStatisticsType<Envelope> STATS_TYPE = new FieldStatisticsType<>(
			"BOUNDING_BOX");

	protected double minX = Double.MAX_VALUE;
	protected double minY = Double.MAX_VALUE;
	protected double maxX = -Double.MAX_VALUE;
	protected double maxY = -Double.MAX_VALUE;

	public BoundingBoxDataStatistics() {
		this(
				null);
	}

	public BoundingBoxDataStatistics(
			final Short adapterId ) {
		super(
				adapterId,
				STATS_TYPE);
	}

	public BoundingBoxDataStatistics(
			final Short adapterId,
			final String fieldName ) {
		super(
				adapterId,
				STATS_TYPE,
				fieldName);
	}

	public boolean isSet() {
		if ((minX == Double.MAX_VALUE) || (minY == Double.MAX_VALUE) || (maxX == -Double.MAX_VALUE)
				|| (maxY == -Double.MAX_VALUE)) {
			return false;
		}
		return true;
	}

	public double getMinX() {
		return minX;
	}

	public double getMinY() {
		return minY;
	}

	public double getMaxX() {
		return maxX;
	}

	public double getMaxY() {
		return maxY;
	}

	public double getWidth() {
		return maxX - minX;
	}

	public double getHeight() {
		return maxY - minY;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(32);
		buffer.putDouble(minX);
		buffer.putDouble(minY);
		buffer.putDouble(maxX);
		buffer.putDouble(maxY);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		minX = buffer.getDouble();
		minY = buffer.getDouble();
		maxX = buffer.getDouble();
		maxY = buffer.getDouble();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... rows ) {
		final Envelope env = getEnvelope(entry);
		if (env != null) {
			minX = Math.min(
					minX,
					env.getMinX());
			minY = Math.min(
					minY,
					env.getMinY());
			maxX = Math.max(
					maxX,
					env.getMaxX());
			maxY = Math.max(
					maxY,
					env.getMaxY());
		}
	}

	public ConstraintSet getConstraints() {
		// Create a NumericRange object using the x axis
		final NumericRange rangeLongitude = new NumericRange(
				minX,
				maxX);

		// Create a NumericRange object using the y axis
		final NumericRange rangeLatitude = new NumericRange(
				minY,
				maxY);

		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerDimension = new HashMap<>();
		// Create and return a new IndexRange array with an x and y axis
		// range
		constraintsPerDimension.put(
				LongitudeDefinition.class,
				new ConstraintData(
						rangeLongitude,
						true));
		constraintsPerDimension.put(
				LatitudeDefinition.class,
				new ConstraintData(
						rangeLatitude,
						true));
		return new ConstraintSet(
				constraintsPerDimension);
	}

	abstract protected Envelope getEnvelope(
			final T entry );

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof BoundingBoxDataStatistics)) {
			final BoundingBoxDataStatistics<T> bboxStats = (BoundingBoxDataStatistics<T>) statistics;
			if (bboxStats.isSet()) {
				minX = Math.min(
						minX,
						bboxStats.minX);
				minY = Math.min(
						minY,
						bboxStats.minY);
				maxX = Math.max(
						maxX,
						bboxStats.maxX);
				maxY = Math.max(
						maxY,
						bboxStats.maxY);
			}
		}
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"bbox[internalAdapter=").append(
				Short.toString(super.getAdapterId()));
		if (isSet()) {
			buffer.append(
					", minX=").append(
					minX);
			buffer.append(
					", maxX=").append(
					maxX);
			buffer.append(
					", minY=").append(
					minY);
			buffer.append(
					", maxY=").append(
					maxY);
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Fixed Bin Numeric statistics to a JSON object
	 */

	@Override
	public Envelope getResult() {
		if (isSet()) {
			return new Envelope(
					minX,
					maxX,
					minY,
					maxY);
		}
		else {
			return new Envelope();
		}
	}

	@Override
	protected String resultsName() {
		return "boundaries";
	}

	@Override
	protected Object resultsValue() {
		if (isSet()) {
			final Map<String, Double> map = new HashMap<>();
			map.put(
					"minX",
					minX);
			map.put(
					"maxX",
					maxX);
			map.put(
					"minY",
					minY);
			map.put(
					"maxY",
					maxY);
			return map;
		}
		else {
			return "No Values";
		}
	}

}
