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
package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.Arrays;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class CoordinateRange implements
		Persistable
{
	private long minCoordinate;
	private long maxCoordinate;
	private byte[] binId;

	protected CoordinateRange() {}

	public CoordinateRange(
			final long minCoordinate,
			final long maxCoordinate,
			final byte[] binId ) {
		this.minCoordinate = minCoordinate;
		this.maxCoordinate = maxCoordinate;
		this.binId = binId;
	}

	public long getMinCoordinate() {
		return minCoordinate;
	}

	public long getMaxCoordinate() {
		return maxCoordinate;
	}

	public byte[] getBinId() {
		return binId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(binId);
		result = (prime * result) + (int) (maxCoordinate ^ (maxCoordinate >>> 32));
		result = (prime * result) + (int) (minCoordinate ^ (minCoordinate >>> 32));
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
		final CoordinateRange other = (CoordinateRange) obj;
		if (!Arrays.equals(
				binId,
				other.binId)) {
			return false;
		}
		if (maxCoordinate != other.maxCoordinate) {
			return false;
		}
		if (minCoordinate != other.minCoordinate) {
			return false;
		}
		return true;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(16 + (binId == null ? 0 : binId.length));
		buf.putLong(minCoordinate);
		buf.putLong(maxCoordinate);
		if (binId != null) {
			buf.put(binId);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		minCoordinate = buf.getLong();
		maxCoordinate = buf.getLong();
		if (bytes.length > 16) {
			binId = new byte[bytes.length - 16];
			buf.get(binId);
		}
		else {
			binId = null;
		}
	}
}
