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
package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

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
	public String getFieldName() {
		return elementField.getFieldName();
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
