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
package mil.nga.giat.geowave.analytic;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.persist.Persistable;

/**
 * 
 * Extracted numeric dimension values associated with an item or a sum of
 * dimension values from multiple items.
 * 
 */
public class GeoObjectDimensionValues implements
		Persistable
{
	public double x = 0.0;
	public double y = 0.0;
	public double z = 0.0;
	public double[] values = new double[0];
	public double distance = 0.0;
	public long count = 0;

	public GeoObjectDimensionValues(
			final int extraValuesCount ) {
		values = new double[extraValuesCount];
	}

	public GeoObjectDimensionValues() {}

	public GeoObjectDimensionValues(
			final double x,
			final double y,
			final double z,
			final double[] extraDimensions,
			final double distance ) {
		super();
		this.x = x;
		this.y = y;
		this.z = z;
		values = extraDimensions;
		this.distance = distance;
		this.count = 1;
	}

	public void add(
			final GeoObjectDimensionValues association ) {
		x += association.x;
		y += association.y;
		z += association.z;
		for (int i = 0; i < values.length; i++) {
			values[i] += association.values[i];
		}
		distance += association.distance;
		count += association.count;
	}

	public void set(
			final double x,
			final double y,
			final double z,
			final double[] extraDimensions,
			final double distance ) {
		this.x = x;
		this.y = y;
		this.z = z;
		values = extraDimensions;
		this.distance = distance;
		count = 1;
	}

	public long getCount() {
		return count;
	}

	public void setCount(
			final long count ) {
		this.count = count;
	}

	public double getX() {
		return x;
	}

	public void setX(
			final double x ) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(
			final double y ) {
		this.y = y;
	}

	public double getZ() {
		return z;
	}

	public void setZ(
			final double z ) {
		this.z = z;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(
			final double distance ) {
		this.distance = distance;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer b = ByteBuffer.allocate(((4 + values.length) * 8) + 4 + 8);
		b.putLong(count);
		b.putDouble(x);
		b.putDouble(y);
		b.putDouble(z);
		b.putDouble(distance);
		b.putInt(values.length);
		for (final double value : values) {
			b.putDouble(value);
		}
		return b.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer b = ByteBuffer.wrap(bytes);
		count = b.getLong();
		x = b.getDouble();
		y = b.getDouble();
		z = b.getDouble();
		distance = b.getDouble();
		int i = b.getInt();
		values = new double[i];
		for (; i > 0; i--) {
			values[i - 1] = b.getDouble();
		}

	}
}
