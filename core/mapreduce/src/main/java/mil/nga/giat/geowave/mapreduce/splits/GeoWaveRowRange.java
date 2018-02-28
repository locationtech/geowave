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
package mil.nga.giat.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;

public class GeoWaveRowRange implements
		Writable
{
	private byte[] partitionKey;
	private byte[] startKey;
	private byte[] endKey;
	private boolean startKeyInclusive;
	private boolean endKeyInclusive;

	protected GeoWaveRowRange() {}

	public GeoWaveRowRange(
			final byte[] partitionKey,
			final byte[] startKey,
			final byte[] endKey,
			final boolean startKeyInclusive,
			final boolean endKeyInclusive ) {
		this.partitionKey = partitionKey;
		this.startKey = startKey;
		this.endKey = endKey;
		this.startKeyInclusive = startKeyInclusive;
		this.endKeyInclusive = endKeyInclusive;
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		out.writeBoolean((partitionKey == null) || (partitionKey.length == 0));
		out.writeBoolean(startKey == null);
		out.writeBoolean(endKey == null);
		if ((partitionKey != null) && (partitionKey.length > 0)) {
			out.writeShort(partitionKey.length);
			out.write(partitionKey);
		}
		if (startKey != null) {
			out.writeShort(startKey.length);
			out.write(startKey);
		}
		if (endKey != null) {
			out.writeShort(endKey.length);
			out.write(endKey);
		}
		out.writeBoolean(startKeyInclusive);
		out.writeBoolean(endKeyInclusive);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		final boolean nullPartitionKey = in.readBoolean();
		final boolean infiniteStartKey = in.readBoolean();
		final boolean infiniteEndKey = in.readBoolean();
		if (!nullPartitionKey) {
			partitionKey = new byte[in.readShort()];
			in.readFully(partitionKey);
		}
		if (!infiniteStartKey) {
			startKey = new byte[in.readShort()];
			in.readFully(startKey);
		}
		else {
			startKey = null;
		}

		if (!infiniteEndKey) {
			endKey = new byte[in.readShort()];
			in.readFully(endKey);
		}
		else {
			endKey = null;
		}

		startKeyInclusive = in.readBoolean();
		endKeyInclusive = in.readBoolean();
	}

	public byte[] getPartitionKey() {
		return partitionKey;
	}

	public byte[] getStartSortKey() {
		return startKey;
	}

	public byte[] getEndSortKey() {
		return endKey;
	}

	public boolean isStartSortKeyInclusive() {
		return startKeyInclusive;
	}

	public boolean isEndSortKeyInclusive() {
		return endKeyInclusive;
	}

	public boolean isInfiniteStartSortKey() {
		return startKey == null;
	}

	public boolean isInfiniteStopSortKey() {
		return endKey == null;
	}

	public byte[] getCombinedStartKey() {
		if ((partitionKey == null) || (partitionKey.length == 0)) {
			return startKey;
		}

		return (startKey == null) ? null : ByteArrayUtils.combineArrays(
				partitionKey,
				startKey);
	}

	public byte[] getCombinedEndKey() {
		if ((partitionKey == null) || (partitionKey.length == 0)) {
			return endKey;
		}

		return (endKey == null) ? ByteArrayUtils.combineArrays(
				ByteArrayId.getNextPrefix(partitionKey),
				endKey) : ByteArrayUtils.combineArrays(
				partitionKey,
				endKey);
	}

	@Override
	public String toString() {
		return "GeoWaveRowRange [partitionKey=" + Arrays.toString(partitionKey) + ", startKey="
				+ Arrays.toString(startKey) + ", endKey=" + Arrays.toString(endKey) + ", startKeyInclusive="
				+ startKeyInclusive + ", endKeyInclusive=" + endKeyInclusive + "]";
	}
}
