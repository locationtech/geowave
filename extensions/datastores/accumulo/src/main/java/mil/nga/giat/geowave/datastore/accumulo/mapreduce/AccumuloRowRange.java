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
package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Range;

import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

public class AccumuloRowRange implements
		GeoWaveRowRange
{

	private Range range;

	public AccumuloRowRange(
			final Range range ) {
		this.range = range;
	}

	public Range getRange() {
		return range;
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		range.write(out);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		range = new Range();
		range.readFields(in);
	}

	@Override
	public byte[] getStartKey() {
		if (range.getStartKey() == null) {
			return null;
		}
		return range.getStartKey().getRowData().getBackingArray();
	}

	@Override
	public byte[] getEndKey() {
		if (range.getEndKey() == null) {
			return null;
		}
		return range.getEndKey().getRowData().getBackingArray();
	}

	@Override
	public boolean isStartKeyInclusive() {
		return range.isStartKeyInclusive();
	}

	@Override
	public boolean isEndKeyInclusive() {
		return range.isEndKeyInclusive();
	}

	@Override
	public boolean isInfiniteStartKey() {
		return range.isInfiniteStartKey();
	}

	@Override
	public boolean isInfiniteStopKey() {
		return range.isInfiniteStopKey();
	}

}
