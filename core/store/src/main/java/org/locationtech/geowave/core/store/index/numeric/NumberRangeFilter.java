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
package org.locationtech.geowave.core.store.index.numeric;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class NumberRangeFilter implements
		QueryFilter
{
	protected String fieldName;
	protected Number lowerValue;
	protected Number upperValue;
	protected boolean inclusiveLow;
	protected boolean inclusiveHigh;

	public NumberRangeFilter() {
		super();
	}

	public NumberRangeFilter(
			final String fieldName,
			final Number lowerValue,
			final Number upperValue,
			final boolean inclusiveLow,
			final boolean inclusiveHigh ) {
		super();
		this.fieldName = fieldName;
		this.lowerValue = lowerValue;
		this.upperValue = upperValue;
		this.inclusiveHigh = inclusiveHigh;
		this.inclusiveLow = inclusiveLow;
	}

	public String getFieldName() {
		return fieldName;
	}

	public Number getLowerValue() {
		return lowerValue;
	}

	public Number getUpperValue() {
		return upperValue;
	}

	public boolean isInclusiveLow() {
		return inclusiveLow;
	}

	public boolean isInclusiveHigh() {
		return inclusiveHigh;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArray value = (ByteArray) persistenceEncoding.getCommonData().getValue(
				fieldName);
		if (value != null) {
			final double val = Lexicoders.DOUBLE.fromByteArray(value.getBytes());
			if (inclusiveLow && inclusiveHigh) {
				return (val >= lowerValue.doubleValue()) && (val <= upperValue.doubleValue());
			}
			else if (inclusiveLow) {
				return (val >= lowerValue.doubleValue()) && (val < upperValue.doubleValue());
			}
			else if (inclusiveHigh) {
				return (val > lowerValue.doubleValue()) && (val <= upperValue.doubleValue());
			}
			else {
				return (val > lowerValue.doubleValue()) && (val < upperValue.doubleValue());
			}
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);
		final ByteBuffer bb = ByteBuffer.allocate(4 + fieldNameBytes.length + 16);
		bb.putInt(fieldNameBytes.length);
		bb.put(fieldNameBytes);
		bb.putDouble(lowerValue.doubleValue());
		bb.putDouble(upperValue.doubleValue());
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] fieldNameBytes = new byte[bb.getInt()];
		bb.get(fieldNameBytes);
		fieldName = StringUtils.stringFromBinary(fieldNameBytes);
		lowerValue = new Double(
				bb.getDouble());
		upperValue = new Double(
				bb.getDouble());

	}
}
