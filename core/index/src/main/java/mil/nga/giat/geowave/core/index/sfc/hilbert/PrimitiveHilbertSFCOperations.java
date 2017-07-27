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
package mil.nga.giat.geowave.core.index.sfc.hilbert;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.uzaygezen.core.BacktrackingQueryBuilder;
import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;
import com.google.uzaygezen.core.FilteredIndexRange;
import com.google.uzaygezen.core.LongContent;
import com.google.uzaygezen.core.PlainFilterCombiner;
import com.google.uzaygezen.core.QueryBuilder;
import com.google.uzaygezen.core.RegionInspector;
import com.google.uzaygezen.core.SimpleRegionInspector;
import com.google.uzaygezen.core.ZoomingSpaceVisitorAdapter;
import com.google.uzaygezen.core.ranges.LongRange;
import com.google.uzaygezen.core.ranges.LongRangeHome;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

/**
 * This supports Compact Hilbert SFC operations using a primitive long
 * internally to represent intermediate results. This can be significantly
 * faster than using unbounded representations such as BigInteger, but can only
 * support up to certain levels of precision. For getID() operations it is
 * currently used if no single dimension is more than 48 bits of precision, and
 * for query decomposition it is currently used if the total precision is <= 62
 * bits.
 *
 *
 */
public class PrimitiveHilbertSFCOperations implements
		HilbertSFCOperations
{
	protected final static long UNIT_CELL_SIZE = (long) Math.pow(
			2,
			19);
	protected long[] binsPerDimension;

	protected long minHilbertValue;
	protected long maxHilbertValue;

	@Override
	public void init(
			final SFCDimensionDefinition[] dimensionDefs ) {
		binsPerDimension = new long[dimensionDefs.length];
		int totalPrecision = 0;
		for (int d = 0; d < dimensionDefs.length; d++) {
			final SFCDimensionDefinition dimension = dimensionDefs[d];
			binsPerDimension[d] = (long) Math.pow(
					2,
					dimension.getBitsOfPrecision());
			totalPrecision += dimension.getBitsOfPrecision();
		}
		minHilbertValue = 0;
		maxHilbertValue = (long) (Math.pow(
				2,
				totalPrecision) - 1);
	}

	@Override
	public byte[] convertToHilbert(
			final double[] values,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {

		final List<Long> dimensionValues = new ArrayList<Long>();

		// Compare the number of dimensions to the number of values sent in
		if (dimensionDefinitions.length != values.length) {
			throw new ArrayIndexOutOfBoundsException(
					"Number of dimensions supplied (" + values.length + ") is different than initialized ("
							+ dimensionDefinitions.length + ").");
		}

		// Loop through each value, then normalize the value based on the
		// dimension definition
		for (int i = 0; i < dimensionDefinitions.length; i++) {
			dimensionValues.add(normalizeDimension(
					dimensionDefinitions[i],
					values[i],
					binsPerDimension[i],
					false,
					false));
		}

		// Convert the normalized values to a BitVector
		final BitVector hilbertBitVector = convertToHilbert(
				dimensionValues,
				compactHilbertCurve,
				dimensionDefinitions);

		return hilbertBitVector.toBigEndianByteArray();
	}

	/***
	 * Converts the incoming values (one per dimension) into a BitVector using
	 * the Compact Hilbert instance. BitVector is a wrapper to allow values
	 * longer than 64 bits.
	 *
	 * @param values
	 *            n-dimensional point to transoform to a point on the hilbert
	 *            SFC
	 * @return point on hilbert SFC
	 */
	private BitVector convertToHilbert(
			final List<Long> values,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final BitVector[] bitVectors = new BitVector[values.size()];

		final BitVector hilbertBitVector = BitVectorFactories.OPTIMAL.apply(compactHilbertCurve
				.getSpec()
				.sumBitsPerDimension());

		for (int i = 0; i < values.size(); i++) {
			bitVectors[i] = BitVectorFactories.OPTIMAL.apply(dimensionDefinitions[i].getBitsOfPrecision());
			bitVectors[i].copyFrom(values.get(i));
		}
		synchronized (compactHilbertCurve) {
			compactHilbertCurve.index(
					bitVectors,
					0,
					hilbertBitVector);
		}
		return hilbertBitVector;
	}

	@Override
	public long[] indicesFromHilbert(
			final byte[] hilbertValue,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		// because it returns an array of longs right now, just use a static
		// method that the unbounded operations can use as well
		return internalIndicesFromHilbert(
				hilbertValue,
				compactHilbertCurve,
				dimensionDefinitions);
	}

	protected static long[] internalIndicesFromHilbert(
			final byte[] hilbertValue,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final BitVector[] perDimensionBitVectors = indexInverse(
				hilbertValue,
				compactHilbertCurve,
				dimensionDefinitions);
		final long[] retVal = new long[dimensionDefinitions.length];
		for (int i = 0; i < retVal.length; i++) {
			retVal[i] = perDimensionBitVectors[i].toExactLong();
		}
		return retVal;
	}

	@Override
	public MultiDimensionalNumericData convertFromHilbert(
			final byte[] hilbertValue,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final BitVector[] perDimensionBitVectors = indexInverse(
				hilbertValue,
				compactHilbertCurve,
				dimensionDefinitions);
		final NumericRange[] retVal = new NumericRange[dimensionDefinitions.length];
		for (int i = 0; i < retVal.length; i++) {
			retVal[i] = denormalizeDimension(
					dimensionDefinitions[i],
					perDimensionBitVectors[i].toExactLong(),
					binsPerDimension[i]);
		}
		return new BasicNumericDataset(
				retVal);
	}

	protected static BitVector[] indexInverse(
			final byte[] hilbertValue,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final BitVector[] perDimensionBitVectors = new BitVector[dimensionDefinitions.length];

		final BitVector hilbertBitVector = BitVectorFactories.OPTIMAL.apply(compactHilbertCurve
				.getSpec()
				.sumBitsPerDimension());
		hilbertBitVector.copyFromBigEndian(hilbertValue);
		for (int i = 0; i < dimensionDefinitions.length; i++) {
			perDimensionBitVectors[i] = BitVectorFactories.OPTIMAL.apply(dimensionDefinitions[i].getBitsOfPrecision());
		}

		synchronized (compactHilbertCurve) {
			compactHilbertCurve.indexInverse(
					hilbertBitVector,
					perDimensionBitVectors);
		}
		return perDimensionBitVectors;
	}

	/***
	 * Used to normalize the value based on the dimension definition, which
	 * includes the dimensional bounds and the bits of precision. This ensures
	 * the maximum amount of fidelity for represented values.
	 *
	 * @param boundedDimensionDefinition
	 *            describes the min, max, and cardinality of a dimension
	 * @param value
	 *            value to be normalized
	 * @param bins
	 *            precomputed number of bins in this dimension the number of
	 *            bins expected based on the cardinality of the definition
	 * @param isMin
	 *            flag indicating if this value is a minimum of a range in which
	 *            case it needs to be inclusive on a boundary, otherwise it is
	 *            exclusive
	 * @return value after normalization
	 * @throws IllegalArgumentException
	 *             thrown when the value passed doesn't fit with in the
	 *             dimension definition provided
	 */
	public long normalizeDimension(
			final SFCDimensionDefinition boundedDimensionDefinition,
			final double value,
			final long bins,
			final boolean isMin,
			final boolean overInclusiveOnEdge )
			throws IllegalArgumentException {
		final double normalizedValue = boundedDimensionDefinition.normalize(value);
		if ((normalizedValue < 0) || (normalizedValue > 1)) {
			throw new IllegalArgumentException(
					"Value (" + value + ") is not within dimension bounds. The normalized value (" + normalizedValue
							+ ") must be within (0,1)");
		}
		// scale it to a value within the bits of precision,
		// because max is handled as exclusive and min is inclusive, we need to
		// handle the edge differently
		if ((isMin && !overInclusiveOnEdge) || (!isMin && overInclusiveOnEdge)) {
			// this will round up on the edge
			return (long) Math.min(
					Math.floor(normalizedValue * bins),
					bins - 1);
		}
		else {
			// this will round down on the edge
			return (long) Math.max(
					Math.ceil(normalizedValue * bins) - 1L,
					0);

		}

	}

	/***
	 * Used to normalize the value based on the dimension definition, which
	 * includes the dimensional bounds and the bits of precision. This ensures
	 * the maximum amount of fidelity for represented values.
	 *
	 * @param boundedDimensionDefinition
	 *            describes the min, max, and cardinality of a dimension
	 * @param value
	 *            hilbert value to be denormalized
	 * @param bins
	 *            precomputed number of bins in this dimension the number of
	 *            bins expected based on the cardinality of the definition
	 * @return range of values representing this hilbert value (exlusive on the
	 *         end)
	 * @throws IllegalArgumentException
	 *             thrown when the value passed doesn't fit with in the hilbert
	 *             SFC for the dimension definition provided
	 */
	private NumericRange denormalizeDimension(
			final SFCDimensionDefinition boundedDimensionDefinition,
			final long value,
			final long bins )
			throws IllegalArgumentException {
		final double min = (double) (value) / (double) bins;
		final double max = (double) (value + 1) / (double) bins;
		if ((min < 0) || (min > 1)) {
			throw new IllegalArgumentException(
					"Value (" + value + ") is not within bounds. The normalized value (" + min
							+ ") must be within (0,1)");
		}
		if ((max < 0) || (max > 1)) {
			throw new IllegalArgumentException(
					"Value (" + value + ") is not within bounds. The normalized value (" + max
							+ ") must be within (0,1)");
		}
		// scale it to a value within the dimension definition range
		return new NumericRange(
				boundedDimensionDefinition.denormalize(min),
				boundedDimensionDefinition.denormalize(max));

	}

	@Override
	public RangeDecomposition decomposeRange(
			final NumericData[] rangePerDimension,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions,
			final int totalPrecision,
			final int maxFilteredIndexedRanges,
			final boolean removeVacuum,
			final boolean overInclusiveOnEdge ) {// List of query range minimum
													// and
		// maximum
		// values
		final List<Long> minRangeList = new ArrayList<Long>();
		final List<Long> maxRangeList = new ArrayList<Long>();

		final LongContent zero = new LongContent(
				0L);
		final List<LongRange> region = new ArrayList<LongRange>(
				dimensionDefinitions.length);
		for (int d = 0; d < dimensionDefinitions.length; d++) {

			final long normalizedMin = normalizeDimension(
					dimensionDefinitions[d],
					rangePerDimension[d].getMin(),
					binsPerDimension[d],
					true,
					overInclusiveOnEdge);
			long normalizedMax = normalizeDimension(
					dimensionDefinitions[d],
					rangePerDimension[d].getMax(),
					binsPerDimension[d],
					false,
					overInclusiveOnEdge);
			if (normalizedMin > normalizedMax) {
				// if they're both equal, which is possible because we treat max
				// as exclusive, set bin max to bin min (ie. treat it as
				// inclusive in this case)
				normalizedMax = normalizedMin;
			}
			minRangeList.add(normalizedMin);
			maxRangeList.add(normalizedMax);
			region.add(LongRange.of(
					normalizedMin,
					normalizedMax + 1L));

		}

		final long minQuadSize = getMinimumQuadSize(
				minRangeList,
				maxRangeList);

		final RegionInspector<LongRange, LongContent> regionInspector = SimpleRegionInspector.create(
				ImmutableList.of(region),
				new LongContent(
						minQuadSize),
				Functions.<LongRange> identity(),
				LongRangeHome.INSTANCE,
				zero);

		final PlainFilterCombiner<LongRange, Long, LongContent, LongRange> intervalCombiner = new PlainFilterCombiner<LongRange, Long, LongContent, LongRange>(
				LongRange.of(
						0,
						1));

		final QueryBuilder<LongRange, LongRange> queryBuilder = BacktrackingQueryBuilder.create(
				regionInspector,
				intervalCombiner,
				maxFilteredIndexedRanges,
				removeVacuum,
				LongRangeHome.INSTANCE,
				zero);
		synchronized (compactHilbertCurve) {
			compactHilbertCurve.accept(new ZoomingSpaceVisitorAdapter(
					compactHilbertCurve,
					queryBuilder));
		}
		final List<FilteredIndexRange<LongRange, LongRange>> hilbertRanges = queryBuilder
				.get()
				.getFilteredIndexRanges();

		final ByteArrayRange[] sfcRanges = new ByteArrayRange[hilbertRanges.size()];
		final int expectedByteCount = (int) Math.ceil(totalPrecision / 8.0);
		if (expectedByteCount <= 0) {
			// special case for no precision
			return new RangeDecomposition(
					new ByteArrayRange[] {
						new ByteArrayRange(
								new ByteArrayId(
										new byte[] {}),
								new ByteArrayId(
										new byte[] {}))
					});
		}
		for (int i = 0; i < hilbertRanges.size(); i++) {
			final FilteredIndexRange<LongRange, LongRange> range = hilbertRanges.get(i);
			// sanity check that values fit within the expected range
			// it seems that uzaygezen can produce a value at 2^totalPrecision
			// rather than 2^totalPrecision - 1
			final long startValue = clamp(
					minHilbertValue,
					maxHilbertValue,
					range.getIndexRange().getStart());
			final long endValue = clamp(
					minHilbertValue,
					maxHilbertValue,
					range.getIndexRange().getEnd() - 1);
			// make sure its padded if necessary
			final byte[] start = HilbertSFC.fitExpectedByteCount(
					expectedByteCount,
					ByteBuffer.allocate(
							8).putLong(
							startValue).array());

			// make sure its padded if necessary
			final byte[] end = HilbertSFC.fitExpectedByteCount(
					expectedByteCount,
					ByteBuffer.allocate(
							8).putLong(
							endValue).array());
			sfcRanges[i] = new ByteArrayRange(
					new ByteArrayId(
							start),
					new ByteArrayId(
							end));
		}

		final RangeDecomposition rangeDecomposition = new RangeDecomposition(
				sfcRanges);

		return rangeDecomposition;
	}

	private static long clamp(
			final long min,
			final long max,
			final long value ) {
		return Math.max(
				Math.min(
						value,
						max),
				0);
	}

	/***
	 * Returns the smallest range that will be fully decomposed (i.e.
	 * decomposition stops when the range is equal or smaller than this value).
	 * Values is based on the _maximumRangeDecompsed and _minRangeDecompsed
	 * instance members.
	 *
	 * @param minRangeList
	 *            minimum values for each dimension (ordered)
	 * @param maxRangeList
	 *            maximum values for each dimension (ordered)
	 * @return largest range that will be fully decomposed
	 */
	private long getMinimumQuadSize(
			final List<Long> minRangeList,
			final List<Long> maxRangeList ) {
		long maxRange = 1;
		final int dimensionality = Math.min(
				minRangeList.size(),
				maxRangeList.size());
		for (int d = 0; d < dimensionality; d++) {
			maxRange = Math.max(
					maxRange,
					(Math.abs(maxRangeList.get(d) - minRangeList.get(d)) + 1));
		}
		final long maxRangeDecomposed = (long) Math.pow(
				maxRange,
				dimensionality);
		if (maxRangeDecomposed <= UNIT_CELL_SIZE) {
			return 1L;
		}

		return maxRangeDecomposed / UNIT_CELL_SIZE;

	}

	/**
	 * The estimated ID count is the cross product of normalized range of all
	 * dimensions per the bits of precision provided by the dimension
	 * definitions.
	 */
	@Override
	public BigInteger getEstimatedIdCount(
			final MultiDimensionalNumericData data,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final double[] mins = data.getMinValuesPerDimension();
		final double[] maxes = data.getMaxValuesPerDimension();
		long estimatedIdCount = 1L;
		for (int d = 0; d < data.getDimensionCount(); d++) {
			final long binMin = normalizeDimension(
					dimensionDefinitions[d],
					mins[d],
					binsPerDimension[d],
					true,
					false);
			long binMax = normalizeDimension(
					dimensionDefinitions[d],
					maxes[d],
					binsPerDimension[d],
					false,
					false);
			if (binMin > binMax) {
				// if they're both equal, which is possible because we treat max
				// as exclusive, set bin max to bin min (ie. treat it as
				// inclusive in this case)
				binMax = binMin;
			}
			estimatedIdCount *= (Math.abs(binMax - binMin) + 1);
		}
		return BigInteger.valueOf(estimatedIdCount);
	}

	@Override
	public double[] getInsertionIdRangePerDimension(
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final double[] retVal = new double[dimensionDefinitions.length];
		for (int i = 0; i < dimensionDefinitions.length; i++) {
			retVal[i] = dimensionDefinitions[i].getRange() / binsPerDimension[i];
		}
		return retVal;
	}

	@Override
	public long[] normalizeRange(
			final double minValue,
			final double maxValue,
			final int dimension,
			final SFCDimensionDefinition boundedDimensionDefinition )
			throws IllegalArgumentException {
		return new long[] {
			normalizeDimension(
					boundedDimensionDefinition,
					minValue,
					binsPerDimension[dimension],
					true,
					true),
			normalizeDimension(
					boundedDimensionDefinition,
					maxValue,
					binsPerDimension[dimension],
					false,
					true)
		};
	}
}
