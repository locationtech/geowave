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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;

/**
 * This class is a concrete implementation of a common index model. Data
 * adapters will map their adapter specific fields to these fields that are
 * common for a given index. This way distributable filters will not need to
 * handle any adapter-specific transformation, but can use the common index
 * fields.
 *
 */
public class CustomCrsIndexModel extends
		BasicIndexModel
{

	private final static Logger LOGGER = LoggerFactory.getLogger(CustomCrsIndexModel.class);
	private String crsCode;
	private CoordinateReferenceSystem crs;

	public CustomCrsIndexModel() {}

	public CustomCrsIndexModel(
			final NumericDimensionField<?>[] dimensions,
			final String crsCode ) {
		init(dimensions);
		this.crsCode = crsCode;
	}

	public CoordinateReferenceSystem getCrs() {
		if (crs == null) {
			try {
				crs = CRS.decode(
						crsCode,
						true);
			}
			catch (final FactoryException e) {
				LOGGER.warn(
						"Unable to decode indexed crs",
						e);
			}
		}
		return crs;
	}

	public String getCrsCode() {
		return crsCode;
	}

	@Override
	public void init(
			final NumericDimensionField<?>[] dimensions ) {
		super.init(dimensions);
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
		final CustomCrsIndexModel other = (CustomCrsIndexModel) obj;
		return Arrays.equals(
				dimensions,
				other.dimensions);
	}

	@Override
	public byte[] toBinary() {
		final byte[] crsCodeBinary = StringUtils.stringToBinary(crsCode);
		int byteBufferLength = 8 + crsCodeBinary.length;
		final List<byte[]> dimensionBinaries = new ArrayList<>(
				dimensions.length);
		for (final NumericDimensionField<?> dimension : dimensions) {
			final byte[] dimensionBinary = PersistenceUtils.toBinary(dimension);
			byteBufferLength += (4 + dimensionBinary.length);
			dimensionBinaries.add(dimensionBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.putInt(dimensions.length);
		buf.putInt(crsCodeBinary.length);
		for (final byte[] dimensionBinary : dimensionBinaries) {
			buf.putInt(dimensionBinary.length);
			buf.put(dimensionBinary);
		}
		buf.put(crsCodeBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numDimensions = buf.getInt();
		final int crsCodeLength = buf.getInt();
		dimensions = new NumericDimensionField[numDimensions];
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			dimensions[i] = (NumericDimensionField<?>) PersistenceUtils.fromBinary(dim);
		}
		final byte[] codeBytes = new byte[crsCodeLength];
		buf.get(codeBytes);
		crsCode = StringUtils.stringFromBinary(codeBytes);
		init(dimensions);
	}

}
