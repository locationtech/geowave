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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * Class that implements a compound index strategy. It's a wrapper around two
 * NumericIndexStrategy objects that can externally be treated as a
 * multi-dimensional NumericIndexStrategy.
 *
 * Each of the 'wrapped' strategies cannot share the same dimension definition.
 *
 */
public class CompoundIndexStrategy implements
		NumericIndexStrategy
{

	private PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> subStrategy1;
	private NumericIndexStrategy subStrategy2;
	private int defaultMaxDuplication;
	private int metaDataSplit = -1;

	public CompoundIndexStrategy(
			final PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> subStrategy1,
			final NumericIndexStrategy subStrategy2 ) {
		this.subStrategy1 = subStrategy1;
		this.subStrategy2 = subStrategy2;
		defaultMaxDuplication = (int) Math.ceil(Math.pow(
				2,
				getNumberOfDimensions()));
	}

	protected CompoundIndexStrategy() {}

	public PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> getPrimarySubStrategy() {
		return subStrategy1;
	}

	public NumericIndexStrategy getSecondarySubStrategy() {
		return subStrategy2;
	}

	@Override
	public byte[] toBinary() {
		final byte[] delegateBinary1 = PersistenceUtils.toBinary(subStrategy1);
		final byte[] delegateBinary2 = PersistenceUtils.toBinary(subStrategy2);
		final ByteBuffer buf = ByteBuffer.allocate(4 + delegateBinary1.length + delegateBinary2.length);
		buf.putInt(delegateBinary1.length);
		buf.put(delegateBinary1);
		buf.put(delegateBinary2);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int delegateBinary1Length = buf.getInt();
		final byte[] delegateBinary1 = new byte[delegateBinary1Length];
		buf.get(delegateBinary1);
		final byte[] delegateBinary2 = new byte[bytes.length - delegateBinary1Length - 4];
		buf.get(delegateBinary2);
		subStrategy1 = (PartitionIndexStrategy) PersistenceUtils.fromBinary(delegateBinary1);
		subStrategy2 = (NumericIndexStrategy) PersistenceUtils.fromBinary(delegateBinary2);

		defaultMaxDuplication = (int) Math.ceil(Math.pow(
				2,
				getNumberOfDimensions()));
	}

	/**
	 * Get the total number of dimensions from all sub-strategies
	 *
	 * @return the number of dimensions
	 */
	public int getNumberOfDimensions() {
		final NumericDimensionDefinition[] dimensions = subStrategy2.getOrderedDimensionDefinitions();
		if (dimensions == null) {
			return 0;
		}
		return dimensions.length;
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				-1,
				hints);
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		final Set<ByteArrayId> partitionIds = subStrategy1.getQueryPartitionKeys(
				indexedRange,
				extractHints(
						hints,
						0));
		final QueryRanges queryRanges = subStrategy2.getQueryRanges(
				indexedRange,
				maxEstimatedRangeDecomposition,
				extractHints(
						hints,
						1));

		return new QueryRanges(
				partitionIds,
				queryRanges);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return getInsertionIds(
				indexedData,
				defaultMaxDuplication);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		final Collection<ByteArrayId> partitionKeys = subStrategy1.getInsertionPartitionKeys(indexedData);
		final InsertionIds insertionIds = subStrategy2.getInsertionIds(
				indexedData,
				maxEstimatedDuplicateIds);

		final boolean partitionKeysEmpty = (partitionKeys == null) || partitionKeys.isEmpty();
		if ((insertionIds == null) || (insertionIds.getPartitionKeys() == null)
				|| insertionIds.getPartitionKeys().isEmpty()) {
			if (partitionKeysEmpty) {
				return new InsertionIds();
			}
			else {
				return new InsertionIds(
						Collections2.transform(
								partitionKeys,
								new Function<ByteArrayId, SinglePartitionInsertionIds>() {
									@Override
									public SinglePartitionInsertionIds apply(
											final ByteArrayId input ) {
										return new SinglePartitionInsertionIds(
												input);
									}
								}));

			}
		}
		else if (partitionKeysEmpty) {
			return insertionIds;
		}
		else {
			final List<SinglePartitionInsertionIds> permutations = new ArrayList<>(
					insertionIds.getPartitionKeys().size() * partitionKeys.size());
			for (final ByteArrayId partitionKey : partitionKeys) {
				permutations.addAll(Collections2.transform(
						insertionIds.getPartitionKeys(),
						new Function<SinglePartitionInsertionIds, SinglePartitionInsertionIds>() {
							@Override
							public SinglePartitionInsertionIds apply(
									final SinglePartitionInsertionIds input ) {
								if (input.getPartitionKey() != null) {
									return new SinglePartitionInsertionIds(
											new ByteArrayId(
													ByteArrayUtils.combineArrays(
															partitionKey.getBytes(),
															input.getPartitionKey().getBytes())),
											input.getSortKeys());
								}
								else {
									return new SinglePartitionInsertionIds(
											partitionKey,
											input.getSortKeys());
								}
							}
						}));
			}
			return new InsertionIds(
					permutations);
		}
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		return subStrategy2.getRangeForId(
				trimPartitionIdForSortStrategy(partitionKey),
				sortKey);
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		return subStrategy2.getCoordinatesPerDimension(
				trimPartitionIdForSortStrategy(partitionKey),
				sortKey);
	}

	private ByteArrayId trimPartitionIdForSortStrategy(
			final ByteArrayId partitionKey ) {
		final ByteArrayId trimmedKey = trimPartitionForSubstrategy(
				subStrategy1.getPartitionKeyLength(),
				false,
				partitionKey);
		return trimmedKey == null ? partitionKey : trimmedKey;
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return subStrategy2.getOrderedDimensionDefinitions();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((subStrategy1 == null) ? 0 : subStrategy1.hashCode());
		result = (prime * result) + ((subStrategy2 == null) ? 0 : subStrategy2.hashCode());
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
		final CompoundIndexStrategy other = (CompoundIndexStrategy) obj;
		if (subStrategy1 == null) {
			if (other.subStrategy1 != null) {
				return false;
			}
		}
		else if (!subStrategy1.equals(other.subStrategy1)) {
			return false;
		}
		if (subStrategy2 == null) {
			if (other.subStrategy2 != null) {
				return false;
			}
		}
		else if (!subStrategy2.equals(other.subStrategy2)) {
			return false;
		}
		return true;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return subStrategy2.getHighestPrecisionIdRangePerDimension();
	}

	@Override
	public int getPartitionKeyLength() {
		return subStrategy1.getPartitionKeyLength() + subStrategy2.getPartitionKeyLength();
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		final List<IndexMetaData> result = new ArrayList<IndexMetaData>();
		for (final IndexMetaData metaData : (List<IndexMetaData>) subStrategy1.createMetaData()) {
			result.add(new CompoundIndexMetaDataWrapper(
					metaData,
					subStrategy1.getPartitionKeyLength(),
					(byte) 0));
		}
		metaDataSplit = result.size();
		for (final IndexMetaData metaData : (List<IndexMetaData>) subStrategy2.createMetaData()) {
			result.add(new CompoundIndexMetaDataWrapper(
					metaData,
					subStrategy1.getPartitionKeyLength(),
					(byte) 1));
		}
		return result;
	}

	private int getMetaDataSplit() {
		if (metaDataSplit == -1) {
			metaDataSplit = subStrategy1.createMetaData().size();
		}
		return metaDataSplit;
	}

	private IndexMetaData[] extractHints(
			final IndexMetaData[] hints,
			final int indexNo ) {
		if ((hints == null) || (hints.length == 0)) {
			return hints;
		}
		final int splitPoint = getMetaDataSplit();
		final int start = (indexNo == 0) ? 0 : splitPoint;
		final int stop = (indexNo == 0) ? splitPoint : hints.length;
		final IndexMetaData[] result = new IndexMetaData[stop - start];
		int p = 0;
		for (int i = start; i < stop; i++) {
			result[p++] = ((CompoundIndexMetaDataWrapper) hints[i]).metaData;
		}
		return result;
	}

	/**
	 *
	 * Delegate Metadata item for an underlying index. For
	 * CompoundIndexStrategy, this delegate wraps the meta data for one of the
	 * two indices. The primary function of this class is to extract out the
	 * parts of the ByteArrayId that are specific to each index during an
	 * 'update' operation.
	 *
	 */
	protected static class CompoundIndexMetaDataWrapper implements
			IndexMetaData
	{

		private IndexMetaData metaData;
		private int partition1Length;
		private byte index;

		protected CompoundIndexMetaDataWrapper() {}

		public CompoundIndexMetaDataWrapper(
				final IndexMetaData metaData,
				final int partition1Length,
				final byte index ) {
			super();
			this.partition1Length = partition1Length;
			this.metaData = metaData;
			this.index = index;
		}

		@Override
		public byte[] toBinary() {
			final byte[] metaBytes = PersistenceUtils.toBinary(metaData);
			final ByteBuffer buf = ByteBuffer.allocate(1 + metaBytes.length);
			buf.put(metaBytes);
			buf.put(index);
			return buf.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			final byte[] metaBytes = new byte[bytes.length - 1];
			buf.get(metaBytes);
			metaData = (IndexMetaData) PersistenceUtils.fromBinary(metaBytes);
			index = buf.get();
		}

		@Override
		public void merge(
				final Mergeable merge ) {
			if (merge instanceof CompoundIndexMetaDataWrapper) {
				final CompoundIndexMetaDataWrapper compound = (CompoundIndexMetaDataWrapper) merge;
				metaData.merge(compound.metaData);
			}
		}

		@Override
		public void insertionIdsAdded(
				final InsertionIds insertionIds ) {
			metaData.insertionIdsAdded(trimPartitionForSubstrategy(insertionIds));
		}

		private InsertionIds trimPartitionForSubstrategy(
				final InsertionIds insertionIds ) {
			final List<SinglePartitionInsertionIds> retVal = new ArrayList<>();
			for (final SinglePartitionInsertionIds partitionIds : insertionIds.getPartitionKeys()) {
				final ByteArrayId trimmedPartitionId = CompoundIndexStrategy.trimPartitionForSubstrategy(
						partition1Length,
						index == 0,
						partitionIds.getPartitionKey());
				if (trimmedPartitionId == null) {
					return insertionIds;
				}
				else {
					retVal.add(new SinglePartitionInsertionIds(
							trimmedPartitionId,
							partitionIds.getSortKeys()));
				}
			}
			return new InsertionIds(
					retVal);
		}

		@Override
		public void insertionIdsRemoved(
				final InsertionIds insertionIds ) {
			metaData.insertionIdsRemoved(trimPartitionForSubstrategy(insertionIds));
		}

		/**
		 * Convert Tiered Index Metadata statistics to a JSON object
		 */

		@Override
		public JSONObject toJSONObject()
				throws JSONException {
			JSONObject jo = new JSONObject();
			jo.put(
					"type",
					"CompoundIndexMetaDataWrapper");
			jo.put(
					"index",
					index);
			return jo;
		}
	}

	/**
	 *
	 * @param partition1Length
	 *            the length of the partition key contributed by the first
	 *            substrategy
	 * @param isFirstSubstrategy
	 *            if the trimming is for the first substrategy
	 * @param compoundPartitionId
	 *            the compound partition id
	 * @return if the partition id requires trimming, the new trimmed key will
	 *         be returned, otherwise if trimming isn't necessary it returns
	 *         null
	 */
	private static ByteArrayId trimPartitionForSubstrategy(
			final int partition1Length,
			final boolean isFirstSubstrategy,
			final ByteArrayId compoundPartitionId ) {
		if ((partition1Length > 0) && ((compoundPartitionId.getBytes().length - partition1Length) > 0)) {
			if (isFirstSubstrategy) {
				return new ByteArrayId(
						Arrays.copyOfRange(
								compoundPartitionId.getBytes(),
								0,
								partition1Length));
			}
			else {
				return new ByteArrayId(
						Arrays.copyOfRange(
								compoundPartitionId.getBytes(),
								partition1Length,
								compoundPartitionId.getBytes().length));
			}
		}
		return null;
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {
		return subStrategy2.getCoordinateRangesPerDimension(
				dataRange,
				hints);
	}

	@Override
	public Set<ByteArrayId> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		final Set<ByteArrayId> partitionKeys1 = subStrategy1.getInsertionPartitionKeys(insertionData);
		final Set<ByteArrayId> partitionKeys2 = subStrategy2.getInsertionPartitionKeys(insertionData);
		if ((partitionKeys1 == null) || partitionKeys1.isEmpty()) {
			return partitionKeys2;
		}
		if ((partitionKeys2 == null) || partitionKeys2.isEmpty()) {
			return partitionKeys1;
		}
		// return permutations
		final Set<ByteArrayId> partitionKeys = new HashSet<>(
				partitionKeys1.size() * partitionKeys2.size());
		for (final ByteArrayId partitionKey1 : partitionKeys1) {
			for (final ByteArrayId partitionKey2 : partitionKeys2) {
				partitionKeys.add(new ByteArrayId(
						ByteArrayUtils.combineArrays(
								partitionKey1.getBytes(),
								partitionKey2.getBytes())));
			}
		}
		return partitionKeys;
	}

	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		final Set<ByteArrayId> partitionKeys1 = subStrategy1.getQueryPartitionKeys(
				queryData,
				hints);
		final Set<ByteArrayId> partitionKeys2 = subStrategy2.getQueryPartitionKeys(
				queryData,
				hints);
		if ((partitionKeys1 == null) || partitionKeys1.isEmpty()) {
			return partitionKeys2;
		}
		if ((partitionKeys2 == null) || partitionKeys2.isEmpty()) {
			return partitionKeys1;
		}
		// return all permutations of partitionKeys
		final Set<ByteArrayId> partitionKeys = new HashSet<ByteArrayId>(
				partitionKeys1.size() * partitionKeys2.size());
		for (final ByteArrayId partitionKey1 : partitionKeys1) {
			for (final ByteArrayId partitionKey2 : partitionKeys2) {
				partitionKeys.add(new ByteArrayId(
						ByteArrayUtils.combineArrays(
								partitionKey1.getBytes(),
								partitionKey2.getBytes())));
			}
		}
		return partitionKeys;
	}
}
