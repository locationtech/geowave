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
package mil.nga.giat.geowave.core.index.sfc.data;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;

/**
 * The Binned Numeric Dataset class creates an object that associates a
 * multi-dimensional index range to a particular bin ID.
 *
 */
public class BinnedNumericDataset implements
		MultiDimensionalNumericData
{
	private byte[] binId;
	private MultiDimensionalNumericData indexRanges;
	private boolean fullExtent;

	public BinnedNumericDataset() {}

	/**
	 *
	 * @param binId
	 *            a unique ID associated with the BinnedQuery object
	 * @param indexRanges
	 *            multi-dimensional range data
	 */
	public BinnedNumericDataset(
			final byte[] binId,
			final MultiDimensionalNumericData indexRanges,
			final boolean fullExtent ) {
		this.binId = binId;
		this.indexRanges = indexRanges;
		this.fullExtent = fullExtent;
	}

	public boolean isFullExtent() {
		return fullExtent;
	}

	/**
	 * @return an array of NumericData objects associated with this object.
	 */
	@Override
	public NumericData[] getDataPerDimension() {
		return indexRanges.getDataPerDimension();
	}

	/**
	 * @return an array of max values associated with each dimension
	 */
	@Override
	public double[] getMaxValuesPerDimension() {
		return indexRanges.getMaxValuesPerDimension();
	}

	/**
	 * @return an array of min values associated with each dimension
	 */
	@Override
	public double[] getMinValuesPerDimension() {
		return indexRanges.getMinValuesPerDimension();
	}

	/**
	 * @return an array of centroid values associated with each dimension
	 */
	@Override
	public double[] getCentroidPerDimension() {
		return indexRanges.getCentroidPerDimension();
	}

	/**
	 * @return the number of total dimensions
	 */
	@Override
	public int getDimensionCount() {
		return indexRanges.getDimensionCount();
	}

	/**
	 * @return a unique ID associated with this object
	 */
	public byte[] getBinId() {
		return binId;
	}

	/**
	 * This method is responsible for translating a query into appropriate
	 * normalized and binned (if necessary) queries that can be used by the
	 * underlying index implementation. For example, for unbounded dimensions
	 * such as time, an incoming query of July 2012 to July 2013 may get
	 * translated into 2 binned queries representing the 2012 portion of the
	 * query and the 2013 portion, each normalized to millis from the beginning
	 * of the year.
	 *
	 * @param numericData
	 *            the incoming query into the index implementation, to be
	 *            translated into normalized, binned queries
	 * @param dimensionDefinitions
	 *            the definition for the dimensions
	 * @return normalized indexes
	 */
	public static BinnedNumericDataset[] applyBins(
			final MultiDimensionalNumericData numericData,
			final NumericDimensionDefinition[] dimensionDefinitions ) {
		if (dimensionDefinitions.length == 0) {
			return new BinnedNumericDataset[0];
		}

		final BinRange[][] binRangesPerDimension = getBinnedRangesPerDimension(
				numericData,
				dimensionDefinitions);
		int numBinnedQueries = 1;
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			numBinnedQueries *= binRangesPerDimension[d].length;
		}
		// now we need to combine all permutations of bin ranges into
		// BinnedQuery objects
		final BinnedNumericDataset[] binnedQueries = new BinnedNumericDataset[numBinnedQueries];
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			for (int b = 0; b < binRangesPerDimension[d].length; b++) {
				for (int i = b; i < numBinnedQueries; i += binRangesPerDimension[d].length) {
					final NumericData[] rangePerDimension;
					if (binnedQueries[i] == null) {
						rangePerDimension = new NumericRange[dimensionDefinitions.length];
						binnedQueries[i] = new BinnedNumericDataset(
								binRangesPerDimension[d][b].getBinId(),
								new BasicNumericDataset(
										rangePerDimension),
								binRangesPerDimension[d][b].isFullExtent());
					}
					else {
						// because binned queries were intended to be immutable,
						// re-instantiate the object
						rangePerDimension = binnedQueries[i].getDataPerDimension();

						final byte[] combinedBinId = ByteArrayUtils.combineArrays(
								binnedQueries[i].getBinId(),
								binRangesPerDimension[d][b].getBinId());
						binnedQueries[i] = new BinnedNumericDataset(
								combinedBinId,
								new BasicNumericDataset(
										rangePerDimension),
								binnedQueries[i].fullExtent |= binRangesPerDimension[d][b].isFullExtent());
					}

					rangePerDimension[d] = new NumericRange(
							binRangesPerDimension[d][b].getNormalizedMin(),
							binRangesPerDimension[d][b].getNormalizedMax());
				}
			}
		}
		return binnedQueries;
	}

	public static BinRange[][] getBinnedRangesPerDimension(
			final MultiDimensionalNumericData numericData,
			final NumericDimensionDefinition[] dimensionDefinitions ) {
		if (dimensionDefinitions.length == 0) {
			return new BinRange[0][];
		}
		final BinRange[][] binRangesPerDimension = new BinRange[dimensionDefinitions.length][];
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			binRangesPerDimension[d] = dimensionDefinitions[d]
					.getNormalizedRanges(numericData.getDataPerDimension()[d]);
		}
		return binRangesPerDimension;
	}

	@Override
	public boolean isEmpty() {
		return indexRanges.isEmpty();
	}

	@Override
	public byte[] toBinary() {
		final byte[] indexRangesBinary = PersistenceUtils.toBinary(indexRanges);
		final ByteBuffer buf = ByteBuffer.allocate(5 + indexRangesBinary.length + binId.length);
		buf.put((byte) (fullExtent ? 1 : 0));
		buf.putInt(binId.length);
		buf.put(binId);
		buf.put(indexRangesBinary);
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		fullExtent = (buf.get() == 1);
		binId = new byte[buf.getInt()];
		buf.get(binId);

		final byte[] indexRangesBinary = new byte[bytes.length - 5 - binId.length];
		buf.get(indexRangesBinary);
		indexRanges = (MultiDimensionalNumericData) PersistenceUtils.fromBinary(indexRangesBinary);
	}
}
