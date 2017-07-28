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
package mil.nga.giat.geowave.core.store.query.aggregate;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.Mergeable;

public class CountResult implements
		Mergeable
{
	protected long count = Long.MIN_VALUE;

	public CountResult() {}

	public CountResult(
			long value ) {
		this.count = value;
	}

	public boolean isSet() {
		return count != Long.MIN_VALUE;
	}

	public long getCount() {
		return count;
	}

	public void setCount(
			long count ) {
		this.count = count;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putLong(count);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		count = buffer.getLong();
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
	public void merge(
			final Mergeable result ) {
		if (!isSet()) {
			count = 0;
		}
		if ((result != null) && (result instanceof CountResult)) {
			final CountResult cStats = (CountResult) result;
			if (cStats.isSet()) {
				count = count + cStats.count;
			}
		}
	}
}
