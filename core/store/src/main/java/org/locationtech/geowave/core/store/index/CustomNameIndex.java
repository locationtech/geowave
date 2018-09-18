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

import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.StringUtils;

public class CustomNameIndex extends
		PrimaryIndex
{
	private String name;

	public CustomNameIndex() {
		super();
	}

	public CustomNameIndex(
			final NumericIndexStrategy indexStrategy,
			final CommonIndexModel indexModel,
			final String name ) {
		super(
				indexStrategy,
				indexModel);
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public byte[] toBinary() {
		final byte[] selfBinary = super.toBinary();
		final byte[] idBinary = StringUtils.stringToBinary(name);
		final ByteBuffer buf = ByteBuffer.allocate(4 + idBinary.length + selfBinary.length);
		buf.putInt(selfBinary.length);
		buf.put(selfBinary);
		buf.put(idBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int selfBinaryLength = buf.getInt();
		final byte[] selfBinary = new byte[selfBinaryLength];
		buf.get(selfBinary);

		super.fromBinary(selfBinary);
		final byte[] nameBinary = new byte[bytes.length - selfBinaryLength - 4];
		buf.get(nameBinary);
		name = StringUtils.stringFromBinary(nameBinary);
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (!(obj instanceof CustomNameIndex)) {
			return false;
		}
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
