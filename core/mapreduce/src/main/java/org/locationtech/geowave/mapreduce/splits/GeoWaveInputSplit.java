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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.locationtech.geowave.core.index.StringUtils;

/**
 * The Class GeoWaveInputSplit. Encapsulates a GeoWave Index and a set of Row
 * ranges for use in Map Reduce jobs.
 */
public class GeoWaveInputSplit extends
		InputSplit implements
		Writable
{
	private Map<String, SplitInfo> splitInfo;
	private String[] locations;

	protected GeoWaveInputSplit() {
		splitInfo = new HashMap<>();
		locations = new String[] {};
	}

	protected GeoWaveInputSplit(
			final Map<String, SplitInfo> splitInfo,
			final String[] locations ) {
		this.splitInfo = splitInfo;
		this.locations = locations;
	}

	public Set<String> getIndexNames() {
		return splitInfo.keySet();
	}

	public SplitInfo getInfo(
			final String indexName ) {
		return splitInfo.get(indexName);
	}

	/**
	 * This implementation of length is only an estimate, it does not provide
	 * exact values. Do not have your code rely on this return value.
	 */
	@Override
	public long getLength()
			throws IOException {
		long diff = 0;
		for (final Entry<String, SplitInfo> indexEntry : splitInfo.entrySet()) {
			for (final RangeLocationPair range : indexEntry.getValue().getRangeLocationPairs()) {
				diff += (long) range.getCardinality();
			}
		}
		return diff;
	}

	@Override
	public String[] getLocations()
			throws IOException {
		return locations;
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		final int numIndices = in.readInt();
		splitInfo = new HashMap<>(
				numIndices);
		for (int i = 0; i < numIndices; i++) {
			final int indexNameLength = in.readInt();
			final byte[] indexNameBytes = new byte[indexNameLength];
			in.readFully(indexNameBytes);
			final String indexName = StringUtils.stringFromBinary(indexNameBytes);
			final SplitInfo si = new SplitInfo();
			si.readFields(in);
			splitInfo.put(
					indexName,
					si);
		}
		final int numLocs = in.readInt();
		locations = new String[numLocs];
		for (int i = 0; i < numLocs; ++i) {
			locations[i] = in.readUTF();
		}
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		out.writeInt(splitInfo.size());
		for (final Entry<String, SplitInfo> range : splitInfo.entrySet()) {
			final byte[] indexNameBytes = StringUtils.stringToBinary(range.getKey());
			out.writeInt(indexNameBytes.length);
			out.write(indexNameBytes);
			final SplitInfo rangeList = range.getValue();
			rangeList.write(out);
		}
		out.writeInt(locations.length);
		for (final String location : locations) {
			out.writeUTF(location);
		}
	}
}
