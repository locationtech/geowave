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
package org.locationtech.geowave.core.store.query.aggregate;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;

public class CountAggregation implements
		CommonIndexAggregation<Persistable, Long>
{
	private long count = 0;

	public CountAggregation() {}

	public boolean isSet() {
		return count != Long.MIN_VALUE;
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"count[count=").append(
				count);
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public void aggregate(
			final CommonIndexedPersistenceEncoding entry ) {
		count++;
	}

	@Override
	public Persistable getParameters() {
		return null;
	}

	@Override
	public Long getResult() {
		return count;
	}

	@Override
	public void setParameters(
			final Persistable parameters ) {}

	@Override
	public void clearResult() {
		count = 0;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public Long merge(
			final Long result1,
			final Long result2 ) {
		return result1 + result2;
	}

	@Override
	public byte[] resultToBinary(
			final Long result ) {
		return ByteBuffer.allocate(
				8).putLong(
				result).array();
	}

	@Override
	public Long resultFromBinary(
			final byte[] binary ) {
		return ByteBuffer.wrap(
				binary).getLong();
	}
}
