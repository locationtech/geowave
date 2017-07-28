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
package mil.nga.giat.geowave.core.index.sfc.xz;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Coordinate;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.binned.BinnedSFCUtils;
import mil.nga.giat.geowave.core.index.sfc.data.BinnedNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy.TierIndexMetaData;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class XZHierarchicalIndexStrategy implements
		HierarchicalNumericIndexStrategy
{
	private final static Logger LOGGER = LoggerFactory.getLogger(XZHierarchicalIndexStrategy.class);

	protected static final int DEFAULT_MAX_RANGES = -1;

	private Byte pointCurveMultiDimensionalId = null;
	private Byte xzCurveMultiDimensionalId = null;

	private SpaceFillingCurve pointCurve;
	private SpaceFillingCurve xzCurve;
	private TieredSFCIndexStrategy rasterStrategy;

	private NumericDimensionDefinition[] baseDefinitions;
	private int[] maxBitsPerDimension;

	private int byteOffsetFromDimensionIndex;

	public XZHierarchicalIndexStrategy() {}

	/**
	 * Constructor used to create a XZ Hierarchical Index Strategy.
	 *
	 * @param maxBitsPerDimension
	 */
	public XZHierarchicalIndexStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final TieredSFCIndexStrategy rasterStrategy,
			final int[] maxBitsPerDimension ) {
		this.rasterStrategy = rasterStrategy;
		this.maxBitsPerDimension = maxBitsPerDimension;
		init(baseDefinitions);
	}

	private void init(
			final NumericDimensionDefinition[] baseDefinitions ) {

		this.baseDefinitions = baseDefinitions;

		byteOffsetFromDimensionIndex = rasterStrategy.getPartitionKeyLength();

		// init dimensionalIds with values not used by rasterStrategy
		for (byte i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
			if (!rasterStrategy.tierExists(i)) {
				if (pointCurveMultiDimensionalId == null) {
					pointCurveMultiDimensionalId = i;
				}
				else if (xzCurveMultiDimensionalId == null) {
					xzCurveMultiDimensionalId = i;
				}
				else {
					break;
				}
			}
		}
		if ((pointCurveMultiDimensionalId == null) || (xzCurveMultiDimensionalId == null)) {
			LOGGER.error("No available byte values for xz and point sfc multiDimensionalIds.");
		}

		final SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[baseDefinitions.length];
		for (int i = 0; i < baseDefinitions.length; i++) {
			sfcDimensions[i] = new SFCDimensionDefinition(
					baseDefinitions[i],
					maxBitsPerDimension[i]);
		}

		pointCurve = SFCFactory.createSpaceFillingCurve(
				sfcDimensions,
				SFCType.HILBERT);
		xzCurve = SFCFactory.createSpaceFillingCurve(
				sfcDimensions,
				SFCType.XZORDER);
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				DEFAULT_MAX_RANGES,
				hints);
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {

		// TODO don't just pass max ranges along to the SFC, take tiering and
		// binning into account to limit the number of ranges correctly

		TierIndexMetaData tieredHints = null;
		XZHierarchicalIndexMetaData xzHints = null;
		if ((hints != null) && (hints.length > 0)) {
			tieredHints = (TierIndexMetaData) hints[0];
			xzHints = (XZHierarchicalIndexMetaData) hints[1];
		}
		final QueryRanges queryRanges = rasterStrategy.getQueryRanges(
				indexedRange,
				maxEstimatedRangeDecomposition,
				tieredHints);

		final BinnedNumericDataset[] binnedQueries = BinnedNumericDataset.applyBins(
				indexedRange,
				baseDefinitions);
		final List<SinglePartitionQueryRanges> partitionedRanges = new ArrayList<>();
		if ((xzHints == null) || (xzHints.pointCurveCount > 0)) {
			partitionedRanges.addAll(BinnedSFCUtils.getQueryRanges(
					binnedQueries,
					pointCurve,
					maxEstimatedRangeDecomposition, // for now we're
													// doing this
													// per SFC rather
													// than
													// dividing by the
													// SFCs
					pointCurveMultiDimensionalId));
		}

		if ((xzHints == null) || (xzHints.xzCurveCount > 0)) {
			partitionedRanges.addAll(BinnedSFCUtils.getQueryRanges(
					binnedQueries,
					xzCurve,
					maxEstimatedRangeDecomposition, // for now we're
													// doing this
													// per SFC rather
													// than
													// dividing by the
													// SFCs
					xzCurveMultiDimensionalId));
		}
		if (partitionedRanges.isEmpty()) {
			return queryRanges;
		}
		final List<QueryRanges> queryRangesList = new ArrayList<>();
		queryRangesList.add(queryRanges);
		queryRangesList.add(new QueryRanges(
				partitionedRanges));
		return new QueryRanges(
				queryRangesList);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {

		final BinnedNumericDataset[] ranges = BinnedNumericDataset.applyBins(
				indexedData,
				baseDefinitions);
		final List<SinglePartitionInsertionIds> partitionIds = new ArrayList<>(
				ranges.length);

		for (final BinnedNumericDataset range : ranges) {
			final BigInteger pointIds = pointCurve.getEstimatedIdCount(range);
			final SinglePartitionInsertionIds pointCurveId = BinnedSFCUtils.getSingleBinnedInsertionId(
					pointIds,
					pointCurveMultiDimensionalId,
					range,
					pointCurve);
			if (pointCurveId != null) {
				partitionIds.add(pointCurveId);
			}
			else {
				final double[] mins = range.getMinValuesPerDimension();
				final double[] maxes = range.getMaxValuesPerDimension();

				final double[] values = new double[mins.length + maxes.length];
				for (int i = 0; i < (values.length - 1); i++) {
					values[i] = mins[i / 2];
					values[i + 1] = maxes[i / 2];
					i++;
				}

				final byte[] xzId = xzCurve.getId(values);

				partitionIds.add(new SinglePartitionInsertionIds(
						new ByteArrayId(
								ByteArrayUtils.combineArrays(
										new byte[] {
											xzCurveMultiDimensionalId
										},
										range.getBinId())),
						new ByteArrayId(
								xzId)));
			}
		}

		return new InsertionIds(
				partitionIds);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		// select curve based on first byte
		final byte first = partitionKey.getBytes()[0];
		if (first == pointCurveMultiDimensionalId) {
			return pointCurve.getRanges(sortKey.getBytes());
		}
		else if (first == xzCurveMultiDimensionalId) {
			return xzCurve.getRanges(sortKey.getBytes());
		}
		else {
			return rasterStrategy.getRangeForId(
					partitionKey,
					sortKey);
		}
	}

	@Override
	public int hashCode() {
		// internal tiered raster strategy already contains all the details that
		// provide uniqueness and comparability to the hierarchical strategy
		return rasterStrategy.hashCode();
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
		final XZHierarchicalIndexStrategy other = (XZHierarchicalIndexStrategy) obj;
		// internal tiered raster strategy already contains all the details that
		// provide uniqueness and comparability to the hierarchical strategy
		return rasterStrategy.equals(other.rasterStrategy);
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public byte[] toBinary() {

		final List<byte[]> dimensionDefBinaries = new ArrayList<byte[]>(
				baseDefinitions.length);
		int bufferLength = 4;
		for (final NumericDimensionDefinition dimension : baseDefinitions) {
			final byte[] sfcDimensionBinary = PersistenceUtils.toBinary(dimension);
			bufferLength += (sfcDimensionBinary.length + 4);
			dimensionDefBinaries.add(sfcDimensionBinary);
		}

		bufferLength += 4;
		final byte[] rasterStrategyBinary = PersistenceUtils.toBinary(rasterStrategy);
		bufferLength += rasterStrategyBinary.length;

		bufferLength += 4;
		bufferLength += maxBitsPerDimension.length * 4;

		final ByteBuffer buf = ByteBuffer.allocate(bufferLength);

		buf.putInt(baseDefinitions.length);
		for (final byte[] dimensionDefBinary : dimensionDefBinaries) {
			buf.putInt(dimensionDefBinary.length);
			buf.put(dimensionDefBinary);
		}

		buf.putInt(rasterStrategyBinary.length);
		buf.put(rasterStrategyBinary);

		buf.putInt(maxBitsPerDimension.length);
		for (final int dimBits : maxBitsPerDimension) {
			buf.putInt(dimBits);
		}

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {

		final ByteBuffer buf = ByteBuffer.wrap(bytes);

		final int numDimensions = buf.getInt();

		baseDefinitions = new NumericDimensionDefinition[numDimensions];
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			baseDefinitions[i] = (NumericDimensionDefinition) PersistenceUtils.fromBinary(dim);
		}

		final int rasterStrategySize = buf.getInt();
		final byte[] rasterStrategyBinary = new byte[rasterStrategySize];
		buf.get(rasterStrategyBinary);
		rasterStrategy = (TieredSFCIndexStrategy) PersistenceUtils.fromBinary(rasterStrategyBinary);

		final int bitsPerDimensionLength = buf.getInt();
		maxBitsPerDimension = new int[bitsPerDimensionLength];
		for (int i = 0; i < bitsPerDimensionLength; i++) {
			maxBitsPerDimension[i] = buf.getInt();
		}

		init(baseDefinitions);
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {

		// select curve based on first byte
		final byte first = partitionKey.getBytes()[0];
		Coordinate[] coordinates = null;

		if (first == pointCurveMultiDimensionalId) {
			coordinates = BinnedSFCUtils.getCoordinatesForId(
					ByteArrayUtils.combineArrays(
							partitionKey.getBytes(),
							sortKey == null ? null : sortKey.getBytes()),
					baseDefinitions,
					pointCurve);
		}
		else if (first == xzCurveMultiDimensionalId) {
			coordinates = BinnedSFCUtils.getCoordinatesForId(
					ByteArrayUtils.combineArrays(
							partitionKey.getBytes(),
							sortKey == null ? null : sortKey.getBytes()),
					baseDefinitions,
					xzCurve);
		}
		else {
			return rasterStrategy.getCoordinatesPerDimension(
					partitionKey,
					sortKey);
		}

		return new MultiDimensionalCoordinates(
				new byte[] {
					first
				},
				coordinates);
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {

		final MultiDimensionalCoordinateRanges[] rasterRanges = rasterStrategy.getCoordinateRangesPerDimension(
				dataRange,
				hints);

		// just pass through raster strategy results since this is only used by
		// raster data for now
		return rasterRanges;

		// final BinRange[][] binRangesPerDimension =
		// BinnedNumericDataset.getBinnedRangesPerDimension(
		// dataRange,
		// baseDefinitions);
		//
		// MultiDimensionalCoordinateRanges[] ranges = new
		// MultiDimensionalCoordinateRanges[rasterRanges.length + 2];
		//
		// ranges[0] = BinnedSFCUtils.getCoordinateRanges(
		// binRangesPerDimension,
		// pointCurve,
		// baseDefinitions.length,
		// pointCurveMultiDimensionalId);
		//
		// ranges[1] = BinnedSFCUtils.getCoordinateRanges(
		// binRangesPerDimension,
		// xzCurve,
		// baseDefinitions.length,
		// xzCurveMultiDimensionalId);
		//
		// System.arraycopy(
		// rasterRanges,
		// 0,
		// ranges,
		// 2,
		// rasterRanges.length);
		//
		// return ranges;
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return baseDefinitions;
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return pointCurve.getInsertionIdRangePerDimension();
	}

	@Override
	public int getPartitionKeyLength() {
		return byteOffsetFromDimensionIndex;
	}

	@Override
	public SubStrategy[] getSubStrategies() {
		return rasterStrategy.getSubStrategies();
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		final List<IndexMetaData> metaData = new ArrayList<IndexMetaData>();
		metaData.addAll(rasterStrategy.createMetaData());
		metaData.add(new XZHierarchicalIndexMetaData(
				pointCurveMultiDimensionalId,
				xzCurveMultiDimensionalId));
		return metaData;
	}

	public static class XZHierarchicalIndexMetaData implements
			IndexMetaData
	{

		private int pointCurveCount = 0;
		private int xzCurveCount = 0;

		private byte pointCurveMultiDimensionalId;
		private byte xzCurveMultiDimensionalId;

		public XZHierarchicalIndexMetaData() {}

		public XZHierarchicalIndexMetaData(
				final byte pointCurveMultiDimensionalId,
				final byte xzCurveMultiDimensionalId ) {
			super();
			this.pointCurveMultiDimensionalId = pointCurveMultiDimensionalId;
			this.xzCurveMultiDimensionalId = xzCurveMultiDimensionalId;
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buffer = ByteBuffer.allocate(2 + (4 * 2));
			buffer.put(pointCurveMultiDimensionalId);
			buffer.put(xzCurveMultiDimensionalId);
			buffer.putInt(pointCurveCount);
			buffer.putInt(xzCurveCount);
			return buffer.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			pointCurveMultiDimensionalId = buffer.get();
			xzCurveMultiDimensionalId = buffer.get();
			pointCurveCount = buffer.getInt();
			xzCurveCount = buffer.getInt();
		}

		@Override
		public void merge(
				final Mergeable merge ) {
			if (merge instanceof XZHierarchicalIndexMetaData) {
				final XZHierarchicalIndexMetaData other = (XZHierarchicalIndexMetaData) merge;
				pointCurveCount += other.pointCurveCount;
				xzCurveCount += other.xzCurveCount;
			}
		}

		@Override
		public void insertionIdsAdded(
				final InsertionIds insertionIds ) {
			for (final SinglePartitionInsertionIds partitionId : insertionIds.getPartitionKeys()) {
				final byte first = partitionId.getPartitionKey().getBytes()[0];
				if (first == pointCurveMultiDimensionalId) {
					pointCurveCount += partitionId.getSortKeys().size();
				}
				else if (first == xzCurveMultiDimensionalId) {
					xzCurveCount += partitionId.getSortKeys().size();
				}
			}
		}

		@Override
		public void insertionIdsRemoved(
				final InsertionIds insertionIds ) {
			for (final SinglePartitionInsertionIds partitionId : insertionIds.getPartitionKeys()) {
				final byte first = partitionId.getPartitionKey().getBytes()[0];
				if (first == pointCurveMultiDimensionalId) {
					pointCurveCount -= partitionId.getSortKeys().size();
				}
				else if (first == xzCurveMultiDimensionalId) {
					xzCurveCount -= partitionId.getSortKeys().size();
				}
			}
		}

		/**
		 * Convert XZHierarchical Index Metadata statistics to a JSON object
		 */

		@Override
		public JSONObject toJSONObject()
				throws JSONException {
			final JSONObject jo = new JSONObject();
			jo.put(
					"type",
					"XZHierarchicalIndexStrategy");

			jo.put(
					"pointCurveMultiDimensionalId",
					pointCurveMultiDimensionalId);
			jo.put(
					"xzCurveMultiDimensionalId",
					xzCurveMultiDimensionalId);
			jo.put(
					"pointCurveCount",
					pointCurveCount);
			jo.put(
					"xzCurveCount",
					xzCurveCount);

			return jo;
		}
	}

	@Override
	public Set<ByteArrayId> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		return IndexUtils.getInsertionPartitionKeys(
				this,
				insertionData);
	}

	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		return IndexUtils.getQueryPartitionKeys(
				this,
				queryData,
				hints);
	}

}
