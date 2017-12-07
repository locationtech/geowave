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
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidObjectException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

public class HBaseRowRange implements
		GeoWaveRowRange
{

	private ByteArrayRange range;
	private boolean infiniteStartKey = false;
	private boolean infiniteEndKey = false;

	public HBaseRowRange(
			final ByteArrayRange range ) {
		if (range == null) {
			infiniteStartKey = true;
			infiniteEndKey = true;
		}
		else {
			ByteArrayId start = range.getStart();
			ByteArrayId end = range.getEnd();

			boolean update = false;

			if (start == null) {
				start = new ByteArrayId(
						HConstants.EMPTY_BYTE_ARRAY);
				update = true;
			}

			if (end == null) {
				end = new ByteArrayId(
						HConstants.EMPTY_BYTE_ARRAY);
				update = true;
			}

			if (update) {
				this.range = new ByteArrayRange(
						start,
						end);
			}
			else {
				this.range = range;
			}

			if (Bytes.equals(
					range.getStart().getBytes(),
					HConstants.EMPTY_BYTE_ARRAY)) {
				infiniteStartKey = true;
			}

			if (Bytes.equals(
					range.getEnd().getBytes(),
					HConstants.EMPTY_BYTE_ARRAY)) {
				infiniteEndKey = true;
			}
		}
	}

	public HBaseRowRange() {
		range = null;
		infiniteStartKey = true;
		infiniteEndKey = true;
	}

	public ByteArrayRange getRange() {
		return range;
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {

		byte[] startBytes = new byte[0];
		byte[] endBytes = new byte[0];

		if (!infiniteStartKey) {
			startBytes = range.getStart().getBytes();
		}
		if (!infiniteEndKey) {
			endBytes = range.getEnd().getBytes();
		}

		out.writeBoolean(infiniteStartKey);
		out.writeBoolean(infiniteEndKey);

		WritableUtils.writeVInt(
				out,
				startBytes.length);
		WritableUtils.writeVInt(
				out,
				endBytes.length);

		out.write(startBytes);
		out.write(endBytes);

	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {

		infiniteStartKey = in.readBoolean();
		infiniteEndKey = in.readBoolean();

		final int startKeyLen = WritableUtils.readVInt(in);
		final int endKeyLen = WritableUtils.readVInt(in);

		byte[] startBytes = new byte[0];
		byte[] endBytes = new byte[0];
		boolean verifyStartBeforeEnd = true;

		if (!infiniteStartKey) {
			startBytes = new byte[startKeyLen];
			in.readFully(startBytes);
		}
		else {
			verifyStartBeforeEnd = false;
		}

		if (!infiniteEndKey) {
			endBytes = new byte[endKeyLen];
			in.readFully(endBytes);
		}
		else {
			verifyStartBeforeEnd = false;
		}

		range = new ByteArrayRange(
				new ByteArrayId(
						startBytes),
				new ByteArrayId(
						endBytes));

		if (verifyStartBeforeEnd && range.getStart().compareTo(
				range.getEnd()) > 0) {
			throw new InvalidObjectException(
					"Start key must be less than end key in range (" + range.getStart().getHexString() + ", "
							+ range.getEnd().getHexString() + ")");
		}

	}

	@Override
	public byte[] getStartKey() {
		if (!infiniteStartKey) {
			return range.getStart().getBytes();
		}
		return null;
	}

	@Override
	public byte[] getEndKey() {
		if (!infiniteEndKey) {
			return range.getEnd().getBytes();
		}
		return null;
	}

	@Override
	public boolean isStartKeyInclusive() {
		return true;
	}

	@Override
	public boolean isEndKeyInclusive() {
		return false;
	}

	@Override
	public boolean isInfiniteStartKey() {
		return infiniteStartKey;
	}

	@Override
	public boolean isInfiniteStopKey() {
		return infiniteEndKey;
	}

}
