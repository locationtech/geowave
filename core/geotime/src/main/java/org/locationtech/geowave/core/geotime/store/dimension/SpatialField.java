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
package org.locationtech.geowave.core.geotime.store.dimension;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

/**
 * A base class for EPSG:4326 latitude/longitude fields that use JTS geometry
 *
 */
abstract public class SpatialField implements
		NumericDimensionField<GeometryWrapper>
{
	protected NumericDimensionDefinition baseDefinition;
	private final GeometryWrapperReader geometryReader;
	private final GeometryWrapperWriter geometryWriter;
	private String fieldName;

	protected SpatialField() {
		geometryReader = new GeometryWrapperReader();
		geometryWriter = new GeometryWrapperWriter();
		this.fieldName = GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME;
	}

	public SpatialField(
			final NumericDimensionDefinition baseDefinition ) {
		this(
				baseDefinition,
				GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME);
	}

	@Override
	public NumericData getFullRange() {
		return baseDefinition.getFullRange();
	}

	public SpatialField(
			final NumericDimensionDefinition baseDefinition,
			final String fieldName ) {
		this.baseDefinition = baseDefinition;
		this.fieldName = fieldName;
		geometryReader = new GeometryWrapperReader();
		geometryWriter = new GeometryWrapperWriter();
	}

	@Override
	public NumericRange getDenormalizedRange(
			final BinRange range ) {
		return new NumericRange(
				range.getNormalizedMin(),
				range.getNormalizedMax());
	}

	@Override
	public double getRange() {
		return baseDefinition.getRange();
	}

	@Override
	public int getFixedBinIdSize() {
		return 0;
	}

	@Override
	public NumericRange getBounds() {
		return baseDefinition.getBounds();
	}

	@Override
	public double normalize(
			final double value ) {
		return baseDefinition.normalize(value);
	}

	@Override
	public double denormalize(
			final double value ) {
		return baseDefinition.denormalize(value);
	}

	@Override
	public BinRange[] getNormalizedRanges(
			final NumericData range ) {
		return baseDefinition.getNormalizedRanges(range);
	}

	@Override
	public String getFieldName() {
		return fieldName;
	}

	@Override
	public FieldWriter<?, GeometryWrapper> getWriter() {
		return geometryWriter;
	}

	@Override
	public FieldReader<GeometryWrapper> getReader() {
		return geometryReader;
	}

	@Override
	public NumericDimensionDefinition getBaseDefinition() {
		return baseDefinition;
	}

	@Override
	public byte[] toBinary() {
		final byte[] dimensionBinary = PersistenceUtils.toBinary(baseDefinition);
		final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);
		final ByteBuffer buf = ByteBuffer.allocate(dimensionBinary.length + fieldNameBytes.length + 4);
		buf.putInt(fieldNameBytes.length);
		buf.put(fieldNameBytes);
		buf.put(dimensionBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int fieldNameLength = buf.getInt();
		final byte[] fieldNameBytes = new byte[fieldNameLength];
		buf.get(fieldNameBytes);
		fieldName = StringUtils.stringFromBinary(fieldNameBytes);
		final byte[] dimensionBinary = new byte[bytes.length - fieldNameLength - 4];
		buf.get(dimensionBinary);
		baseDefinition = (NumericDimensionDefinition) PersistenceUtils.fromBinary(dimensionBinary);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final String className = getClass().getName();
		result = (prime * result) + ((className == null) ? 0 : className.hashCode());
		result = (prime * result) + ((baseDefinition == null) ? 0 : baseDefinition.hashCode());
		result = (prime * result) + ((fieldName == null) ? 0 : fieldName.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final SpatialField other = (SpatialField) obj;
		if (baseDefinition == null) {
			if (other.baseDefinition != null) {
				return false;
			}
		}
		else if (!baseDefinition.equals(other.baseDefinition)) {
			return false;
		}
		if (fieldName == null) {
			if (other.fieldName != null) {
				return false;
			}
		}
		else if (!fieldName.equals(other.fieldName)) {
			return false;
		}
		return true;
	}
}
