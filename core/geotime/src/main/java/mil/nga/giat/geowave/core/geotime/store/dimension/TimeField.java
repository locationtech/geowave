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

import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

/**
 * This field definition can be used for temporal data (either as a time range
 * or a single instant in time).
 * 
 */
public class TimeField implements
		NumericDimensionField<Time>
{
	// DOUBLE null terminate to be extra sure this is a reserved field ID
	private final static ByteArrayId DEFAULT_FIELD_ID = new ByteArrayId(
			ByteArrayUtils.combineArrays(
					StringUtils.stringToBinary("time"),
					new byte[] {
						0,
						0
					}));
	private NumericDimensionDefinition baseDefinition;
	private final TimeAdapter adapter;
	private ByteArrayId fieldId;

	public TimeField() {
		adapter = new TimeAdapter();
	}

	public TimeField(
			final Unit timeUnit ) {
		this(
				timeUnit,
				DEFAULT_FIELD_ID);
	}

	public TimeField(
			final Unit timeUnit,
			final ByteArrayId fieldId ) {
		this(
				new TimeDefinition(
						timeUnit),
				fieldId);
	}

	@Override
	public NumericData getFullRange() {
		return new NumericRange(
				0,
				System.currentTimeMillis() + 1);
	}

	public TimeField(
			final NumericDimensionDefinition baseDefinition,
			final ByteArrayId fieldId ) {
		this.baseDefinition = baseDefinition;
		adapter = new TimeAdapter();
		this.fieldId = fieldId;
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
			final NumericData index ) {
		return baseDefinition.getNormalizedRanges(index);
	}

	@Override
	public NumericRange getDenormalizedRange(
			final BinRange range ) {
		return baseDefinition.getDenormalizedRange(range);
	}

	@Override
	public int getFixedBinIdSize() {
		return baseDefinition.getFixedBinIdSize();
	}

	@Override
	public double getRange() {
		return baseDefinition.getRange();
	}

	@Override
	public NumericRange getBounds() {
		return baseDefinition.getBounds();
	}

	@Override
	public NumericData getNumericData(
			final Time dataElement ) {
		return dataElement.toNumericData();
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public FieldWriter<?, Time> getWriter() {
		return adapter;
	}

	@Override
	public FieldReader<Time> getReader() {
		return adapter;
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
		final TimeField other = (TimeField) obj;
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
