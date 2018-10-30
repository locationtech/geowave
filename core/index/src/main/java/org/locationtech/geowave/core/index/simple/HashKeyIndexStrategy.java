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
package org.locationtech.geowave.core.index.simple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.PartitionIndexStrategy;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;

import com.google.common.collect.Sets;

/**
 * Used to create determined, uniform row id prefix as one possible approach to
 * prevent hot spotting.
 *
 * Before using this class, one should consider balancing options for the
 * specific data store. Can one pre-split using a component of another index
 * strategy (e.g. bin identifier)? How about ingest first and then do major
 * compaction?
 *
 * Consider that Accumulo 1.7 supports two balancers
 * org.apache.accumulo.server.master.balancer.RegexGroupBalancer and
 * org.apache.accumulo.server.master.balancer.GroupBalancer.
 *
 * This class should be used with a CompoundIndexStrategy. In addition, tablets
 * should be pre-split on the number of prefix IDs. Without splits, the splits
 * are at the mercy of the Big Table servers default. For example, Accumulo
 * fills up one tablet before splitting, regardless of the partitioning.
 *
 * The key set size does not need to be large. For example, using two times the
 * number of tablet servers (for growth) and presplitting, two keys per server.
 * The default is 3.
 *
 * There is a cost to using this approach: queries must span all prefixes. The
 * number of prefixes should initially be at least the number of tablet servers.
 *
 *
 *
 */
public class HashKeyIndexStrategy implements
		PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData>
{

	private final List<ByteArray> keys = new ArrayList<ByteArray>();

	public HashKeyIndexStrategy() {
		this(
				3);
	}

	public HashKeyIndexStrategy(
			final int size ) {
		init(size);
	}

	private void init(
			final int size ) {
		keys.clear();
		if (size > 256) {
			final ByteBuffer buf = ByteBuffer.allocate(4);
			for (int i = 0; i < size; i++) {
				buf.putInt(i);
				final ByteArray id = new ByteArray(
						Arrays.copyOf(
								buf.array(),
								4));
				keys.add(id);
				buf.rewind();
			}
		}
		else {
			for (int i = 0; i < size; i++) {
				final ByteArray id = new ByteArray(
						new byte[] {
							(byte) i
						});
				keys.add(id);
			}
		}
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(keys.size());
		return buf.array();

	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		init(buf.getInt());
	}

	public Set<ByteArray> getPartitionKeys() {
		return Sets.newHashSet(keys);
	}

	private static long hashCode(
			final double a1[],
			final long start ) {
		long result = start;
		for (final double element : a1) {
			final long bits = Double.doubleToLongBits(element);
			result = (31 * result) + (bits ^ (bits >>> 32));
		}
		return result;
	}

	@Override
	public int getPartitionKeyLength() {
		if ((keys != null) && !keys.isEmpty()) {
			return keys.get(
					0).getBytes().length;
		}
		return 0;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	/**
	 * Returns an insertion id selected round-robin from a predefined pool
	 *
	 */
	@Override
	public Set<ByteArray> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		final long hashCode = Math.abs(hashCode(
				insertionData.getMaxValuesPerDimension(),
				hashCode(
						insertionData.getMinValuesPerDimension(),
						1)));
		final int position = (int) (hashCode % keys.size());

		return Collections.singleton(keys.get(position));
	}

	/**
	 * always return all keys
	 */
	@Override
	public Set<ByteArray> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		return getPartitionKeys();
	}

	@Override
	public Set<ByteArray> getPredefinedSplits() {
		return getPartitionKeys();
	}
}
