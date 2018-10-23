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
package org.locationtech.geowave.core.store.entities;

import java.util.Arrays;

import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.util.DataStoreUtils;

public class GeoWaveValueImpl implements
		GeoWaveValue
{
	private final byte[] fieldMask;
	private final byte[] visibility;
	private final byte[] value;

	public GeoWaveValueImpl(
			final GeoWaveValue[] values ) {
		if ((values == null) || (values.length == 0)) {
			fieldMask = new byte[] {};
			visibility = new byte[] {};
			value = new byte[] {};
		}
		else if (values.length == 1) {
			fieldMask = values[0].getFieldMask();
			visibility = values[0].getVisibility();
			value = values[0].getValue();
		}
		else {
			byte[] intermediateFieldMask = values[0].getFieldMask();
			byte[] intermediateVisibility = values[0].getVisibility();
			byte[] intermediateValue = values[0].getValue();
			for (int i = 1; i < values.length; i++) {
				intermediateFieldMask = BitmaskUtils.generateANDBitmask(
						intermediateFieldMask,
						values[i].getFieldMask());
				intermediateVisibility = DataStoreUtils.mergeVisibilities(
						intermediateVisibility,
						values[i].getVisibility());
				intermediateValue = ByteArrayUtils.combineArrays(
						intermediateValue,
						values[i].getValue());

			}
			fieldMask = intermediateFieldMask;
			visibility = intermediateVisibility;
			value = intermediateValue;
		}
	}

	public GeoWaveValueImpl(
			final byte[] fieldMask,
			final byte[] visibility,
			final byte[] value ) {
		this.fieldMask = fieldMask;
		this.visibility = visibility;
		this.value = value;
	}

	@Override
	public byte[] getFieldMask() {
		return fieldMask;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public byte[] getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(fieldMask);
		result = (prime * result) + Arrays.hashCode(value);
		result = (prime * result) + Arrays.hashCode(visibility);
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
		final GeoWaveValueImpl other = (GeoWaveValueImpl) obj;
		if (!Arrays.equals(
				fieldMask,
				other.fieldMask)) {
			return false;
		}
		if (!Arrays.equals(
				value,
				other.value)) {
			return false;
		}
		if (!Arrays.equals(
				visibility,
				other.visibility)) {
			return false;
		}
		return true;
	}
}
