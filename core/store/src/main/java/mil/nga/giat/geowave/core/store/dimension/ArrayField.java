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
package mil.nga.giat.geowave.core.store.dimension;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

abstract public class ArrayField<T extends CommonIndexValue> implements
		NumericDimensionField<ArrayWrapper<T>>
{
	protected NumericDimensionField<T> elementField;

	public ArrayField(
			final NumericDimensionField<T> elementField ) {
		this.elementField = elementField;
	}

	public ArrayField() {}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(elementField);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		elementField = (NumericDimensionField<T>) PersistenceUtils.fromBinary(bytes);
	}

	@Override
	public double getRange() {
		return elementField.getRange();
	}

	@Override
	public double normalize(
			final double value ) {
		return elementField.normalize(value);
	}

	@Override
	public double denormalize(
			final double value ) {
		return elementField.denormalize(value);
	}

	@Override
	public ByteArrayId getFieldId() {
		return elementField.getFieldId();
	}

	@Override
	public BinRange[] getNormalizedRanges(
			final NumericData range ) {
		return elementField.getNormalizedRanges(range);
	}

	@Override
	public NumericRange getDenormalizedRange(
			final BinRange range ) {
		return elementField.getDenormalizedRange(range);
	}

	@Override
	public NumericData getNumericData(
			final ArrayWrapper<T> dataElement ) {
		Double min = null, max = null;
		for (final T element : dataElement.getArray()) {
			final NumericData data = elementField.getNumericData(element);
			if ((min == null) || (max == null)) {
				min = data.getMin();
				max = data.getMax();
			}
			else {
				min = Math.min(
						min,
						data.getMin());
				max = Math.max(
						max,
						data.getMax());
			}
		}
		if ((min == null) || (max == null)) {
			return null;
		}
		if (min.equals(max)) {
			return new NumericValue(
					min);
		}
		return new NumericRange(
				min,
				max);
	}

	@Override
	abstract public FieldWriter<?, ArrayWrapper<T>> getWriter();

	@Override
	abstract public FieldReader<ArrayWrapper<T>> getReader();

	@Override
	public NumericDimensionDefinition getBaseDefinition() {
		return elementField.getBaseDefinition();
	}

	@Override
	public int getFixedBinIdSize() {
		return elementField.getFixedBinIdSize();
	}

	@Override
	public NumericRange getBounds() {
		return elementField.getBounds();
	}

	@Override
	public NumericData getFullRange() {
		return elementField.getFullRange();
	}

}
