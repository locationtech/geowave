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
package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NullNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

/**
 * This can be used as a pass-through for an index. In other words, it
 * represents an index with no dimensions. It will create a GeoWave-compliant
 * table named with the provided ID and primarily useful to access the data by
 * row ID. Because it has no dimensions, range scans will result in full table
 * scans.
 * 
 * 
 */
public class NullIndex extends
		PrimaryIndex
{

	public NullIndex() {
		super();
	}

	public NullIndex(
			final String id ) {
		super(
				new NullNumericIndexStrategy(
						id),
				new BasicIndexModel(
						new NumericDimensionField[] {}));
	}

	@Override
	public ByteArrayId getId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(indexStrategy.getId()));
	}

	@Override
	public byte[] toBinary() {
		return StringUtils.stringToBinary(indexStrategy.getId());
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		indexModel = new BasicIndexModel(
				new NumericDimensionField[] {});
		indexStrategy = new NullNumericIndexStrategy(
				StringUtils.stringFromBinary(bytes));
	}
}
