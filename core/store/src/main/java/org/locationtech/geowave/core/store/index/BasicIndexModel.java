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
package org.locationtech.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

/**
 * This class is a concrete implementation of a common index model. Data
 * adapters will map their adapter specific fields to these fields that are
 * common for a given index. This way distributable filters will not need to
 * handle any adapter-specific transformation, but can use the common index
 * fields.
 *
 */
public class BasicIndexModel implements
		CommonIndexModel
{
	protected NumericDimensionField<?>[] dimensions;
	// the first dimension of a particular field ID will be the persistence
	// model used
	private Map<String, NumericDimensionField<?>> fieldIdToPeristenceMap;
	private transient String id;

	public BasicIndexModel() {}

	public BasicIndexModel(
			final NumericDimensionField<?>[] dimensions ) {
		init(dimensions);
	}

	public void init(
			final NumericDimensionField<?>[] dimensions ) {
		this.dimensions = dimensions;
		fieldIdToPeristenceMap = new HashMap<>();
		for (final NumericDimensionField<?> d : dimensions) {
			if (!fieldIdToPeristenceMap.containsKey(d.getFieldName())) {
				fieldIdToPeristenceMap.put(
						d.getFieldName(),
						d);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldWriter<Object, CommonIndexValue> getWriter(
			final String fieldName ) {
		final NumericDimensionField<?> dimension = fieldIdToPeristenceMap.get(fieldName);
		if (dimension != null) {
			return (FieldWriter<Object, CommonIndexValue>) dimension.getWriter();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldReader<CommonIndexValue> getReader(
			final String fieldName ) {
		final NumericDimensionField<?> dimension = fieldIdToPeristenceMap.get(fieldName);
		if (dimension != null) {
			return (FieldReader<CommonIndexValue>) dimension.getReader();
		}
		return null;
	}

	@Override
	public NumericDimensionField<? extends CommonIndexValue>[] getDimensions() {
		return dimensions;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final String className = getClass().getName();
		result = (prime * result) + ((className == null) ? 0 : className.hashCode());
		result = (prime * result) + Arrays.hashCode(dimensions);
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
		final BasicIndexModel other = (BasicIndexModel) obj;
		return Arrays.equals(
				dimensions,
				other.dimensions);
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 4;
		final List<byte[]> dimensionBinaries = new ArrayList<>(
				dimensions.length);
		for (final NumericDimensionField<?> dimension : dimensions) {
			final byte[] dimensionBinary = PersistenceUtils.toBinary(dimension);
			byteBufferLength += (4 + dimensionBinary.length);
			dimensionBinaries.add(dimensionBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.putInt(dimensions.length);
		for (final byte[] dimensionBinary : dimensionBinaries) {
			buf.putInt(dimensionBinary.length);
			buf.put(dimensionBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numDimensions = buf.getInt();
		dimensions = new NumericDimensionField[numDimensions];
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			dimensions[i] = (NumericDimensionField<?>) PersistenceUtils.fromBinary(dim);
		}
		init(dimensions);
	}

	@Override
	public String getId() {
		if (id == null) {
			id = StringUtils.intToString(hashCode());
		}
		return id;
	}
}
