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
package mil.nga.giat.geowave.core.geotime.store.dimension;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

/**
 * A base class for EPSG:4326 latitude/longitude fields that use JTS geometry
 * 
 */
abstract public class SpatialField implements
		NumericDimensionField<GeometryWrapper>
{
	protected NumericDimensionDefinition baseDefinition;
	private final GeometryAdapter geometryAdapter;
	private ByteArrayId fieldId;

	protected SpatialField() {
		geometryAdapter = new GeometryAdapter();
	}

	public SpatialField(
			final NumericDimensionDefinition baseDefinition ) {
		this(
				baseDefinition,
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
	}

	@Override
	public NumericData getFullRange() {
		return baseDefinition.getFullRange();
	}

	public SpatialField(
			final NumericDimensionDefinition baseDefinition,
			final ByteArrayId fieldId ) {
		this.baseDefinition = baseDefinition;
		this.fieldId = fieldId;
		geometryAdapter = new GeometryAdapter();
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
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public FieldWriter<?, GeometryWrapper> getWriter() {
		return geometryAdapter;
	}

	@Override
	public FieldReader<GeometryWrapper> getReader() {
		return geometryAdapter;
	}

	@Override
	public NumericDimensionDefinition getBaseDefinition() {
		return baseDefinition;
	}

	@Override
	public byte[] toBinary() {
		final byte[] dimensionBinary = PersistenceUtils.toBinary(baseDefinition);
		final ByteBuffer buf = ByteBuffer.allocate(dimensionBinary.length + fieldId.getBytes().length + 4);
		buf.putInt(fieldId.getBytes().length);
		buf.put(fieldId.getBytes());
		buf.put(dimensionBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int fieldIdLength = buf.getInt();
		final byte[] fieldIdBinary = new byte[fieldIdLength];
		buf.get(fieldIdBinary);
		fieldId = new ByteArrayId(
				fieldIdBinary);

		final byte[] dimensionBinary = new byte[bytes.length - fieldIdLength - 4];
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
		result = (prime * result) + ((fieldId == null) ? 0 : fieldId.hashCode());
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
		if (fieldId == null) {
			if (other.fieldId != null) {
				return false;
			}
		}
		else if (!fieldId.equals(other.fieldId)) {
			return false;
		}
		return true;
	}
}
