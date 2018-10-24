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

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;

/**
 * This class fully describes everything necessary to index data within GeoWave.
 * The key components are the indexing strategy and the common index model.
 */
public class PrimaryIndex implements
		Index
{
	protected NumericIndexStrategy indexStrategy;
	protected CommonIndexModel indexModel;

	public PrimaryIndex() {}

	public PrimaryIndex(
			final NumericIndexStrategy indexStrategy,
			final CommonIndexModel indexModel ) {
		this.indexStrategy = indexStrategy;
		this.indexModel = indexModel;
	}

	public NumericIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public CommonIndexModel getIndexModel() {
		return indexModel;
	}

	public String getName() {
		return indexStrategy.getId() + "_" + indexModel.getId();
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
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
		final PrimaryIndex other = (PrimaryIndex) obj;
		return getName().equals(
				other.getName());
	}

	@Override
	public byte[] toBinary() {
		final byte[] indexStrategyBinary = PersistenceUtils.toBinary(indexStrategy);
		final byte[] indexModelBinary = PersistenceUtils.toBinary(indexModel);
		final ByteBuffer buf = ByteBuffer.allocate(indexStrategyBinary.length + indexModelBinary.length + 4);
		buf.putInt(indexStrategyBinary.length);
		buf.put(indexStrategyBinary);
		buf.put(indexModelBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int indexStrategyLength = buf.getInt();
		final byte[] indexStrategyBinary = new byte[indexStrategyLength];
		buf.get(indexStrategyBinary);

		indexStrategy = (NumericIndexStrategy) PersistenceUtils.fromBinary(indexStrategyBinary);

		final byte[] indexModelBinary = new byte[bytes.length - indexStrategyLength - 4];
		buf.get(indexModelBinary);
		indexModel = (CommonIndexModel) PersistenceUtils.fromBinary(indexModelBinary);
	}
}
