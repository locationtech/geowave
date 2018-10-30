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
package org.locationtech.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;

public class SplitInfo
{
	private Index index;
	private List<RangeLocationPair> rangeLocationPairs;
	private boolean mixedVisibility = true;
	private boolean authorizationsLimiting = true;

	protected SplitInfo() {}

	public SplitInfo(
			final Index index ) {
		this.index = index;
		rangeLocationPairs = new ArrayList<RangeLocationPair>();
	}

	public SplitInfo(
			final Index index,
			final List<RangeLocationPair> rangeLocationPairs ) {
		super();
		this.index = index;
		this.rangeLocationPairs = rangeLocationPairs;
	}

	public boolean isMixedVisibility() {
		return mixedVisibility;
	}

	public void setMixedVisibility(
			final boolean mixedVisibility ) {
		this.mixedVisibility = mixedVisibility;
	}

	public boolean isAuthorizationsLimiting() {
		return authorizationsLimiting;
	}

	public void setAuthorizationsLimiting(
			boolean authorizationsLimiting ) {
		this.authorizationsLimiting = authorizationsLimiting;
	}

	public Index getIndex() {
		return index;
	}

	public List<RangeLocationPair> getRangeLocationPairs() {
		return rangeLocationPairs;
	}

	public void readFields(
			final DataInput in )
			throws IOException {
		final int indexLength = in.readInt();
		final byte[] indexBytes = new byte[indexLength];
		in.readFully(indexBytes);
		final Index index = (Index) PersistenceUtils.fromBinary(indexBytes);
		final int numRanges = in.readInt();
		final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>(
				numRanges);

		for (int j = 0; j < numRanges; j++) {
			try {
				final RangeLocationPair range = new RangeLocationPair();
				range.readFields(in);
				rangeList.add(range);
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new IOException(
						"Unable to instantiate range",
						e);
			}
		}
		this.index = index;
		rangeLocationPairs = rangeList;
		mixedVisibility = in.readBoolean();
		authorizationsLimiting = in.readBoolean();
	}

	public void write(
			final DataOutput out )
			throws IOException {
		final byte[] indexBytes = PersistenceUtils.toBinary(index);
		out.writeInt(indexBytes.length);
		out.write(indexBytes);
		out.writeInt(rangeLocationPairs.size());
		for (final RangeLocationPair r : rangeLocationPairs) {
			r.write(out);
		}
		out.writeBoolean(mixedVisibility);
		out.writeBoolean(authorizationsLimiting);
	}
}
