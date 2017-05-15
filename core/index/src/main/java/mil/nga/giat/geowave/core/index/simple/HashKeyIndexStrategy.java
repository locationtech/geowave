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
package mil.nga.giat.geowave.core.index.simple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

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
		NumericIndexStrategy
{

	private final List<ByteArrayRange> keySet = new ArrayList<ByteArrayRange>();

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
		keySet.clear();
		if (size > 256) {
			final ByteBuffer buf = ByteBuffer.allocate(4);
			for (int i = 0; i < size; i++) {
				buf.putInt(i);
				final ByteArrayId id = new ByteArrayId(
						Arrays.copyOf(
								buf.array(),
								4));
				keySet.add(new ByteArrayRange(
						id,
						id));
				buf.rewind();
			}
		}
		else {
			for (int i = 0; i < size; i++) {
				final ByteArrayId id = new ByteArrayId(
						new byte[] {
							(byte) i
						});
				keySet.add(new ByteArrayRange(
						id,
						id));
			}
		}
	}

	/**
	 * Always returns all possible ranges
	 *
	 * {@inheritDoc}
	 */
	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return keySet;
	}

	/**
	 * Always returns all possible ranges
	 */
	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return keySet;
	}

	/**
	 * Returns an insertion id selected round-robin from a predefined pool
	 *
	 */
	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		final long hashCode = Math.abs(hashCode(
				indexedData.getMaxValuesPerDimension(),
				hashCode(
						indexedData.getMinValuesPerDimension(),
						1)));
		final int position = (int) (hashCode % keySet.size());

		return Collections.singletonList(keySet.get(
				position).getStart());
	}

	/**
	 * Returns all of the insertion ids for the range. Since this index strategy
	 * doensn't use binning, it will return the ByteArrayId of every value in
	 * the range (i.e. if you are storing a range using this index strategy,
	 * your data will be replicated for every integer value in the range).
	 *
	 * {@inheritDoc}
	 */
	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return new NumericDimensionDefinition[0];
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		return new BasicNumericDataset();
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return new double[0];
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(keySet.size());
		return buf.array();

	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		init(buf.getInt());
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		final Set<ByteArrayId> naturalSplits = new HashSet<ByteArrayId>();
		for (final ByteArrayRange range : keySet) {
			naturalSplits.add(range.getStart());
		}
		return naturalSplits;
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
	public int getByteOffsetFromDimensionalIndex() {
		if ((keySet != null) && !keySet.isEmpty()) {
			return keySet.get(
					0).getStart().getBytes().length;
		}
		return 0;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		return new MultiDimensionalCoordinates();
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {
		return new MultiDimensionalCoordinateRanges[] {
			new MultiDimensionalCoordinateRanges()
		};
	}
}
