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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.uzaygezen.core.BacktrackingQueryBuilder;
import com.google.uzaygezen.core.BigIntegerContent;
import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;
import com.google.uzaygezen.core.FilteredIndexRange;
import com.google.uzaygezen.core.PlainFilterCombiner;
import com.google.uzaygezen.core.QueryBuilder;
import com.google.uzaygezen.core.RegionInspector;
import com.google.uzaygezen.core.SimpleRegionInspector;
import com.google.uzaygezen.core.ZoomingSpaceVisitorAdapter;
import com.google.uzaygezen.core.ranges.BigIntegerRange;
import com.google.uzaygezen.core.ranges.BigIntegerRangeHome;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

/**
 * This supports Compact Hilbert SFC operations using a BigInteger internally to
 * represent intermediate results. This can be significantly slower than using a
 * primitive long for intermediate results but can support arbitrarily many bits
 * of precision.
 *
 *
 */
public class UnboundedHilbertSFCOperations implements
		HilbertSFCOperations
{
	private static final BigDecimal TWO = BigDecimal.valueOf(2);
	protected final static BigInteger UNIT_CELL_SIZE = BigDecimal.valueOf(
			Math.pow(
					2,
					19)).toBigInteger();
	protected BigDecimal[] binsPerDimension;
	protected BigInteger minHilbertValue;
	protected BigInteger maxHilbertValue;

	@Override
	public void init(
			final SFCDimensionDefinition[] dimensionDefs ) {
		binsPerDimension = new BigDecimal[dimensionDefs.length];
		int totalPrecision = 0;
		for (int d = 0; d < dimensionDefs.length; d++) {
			final SFCDimensionDefinition dimension = dimensionDefs[d];
			binsPerDimension[d] = TWO.pow(dimension.getBitsOfPrecision());
			totalPrecision += dimension.getBitsOfPrecision();
		}
		minHilbertValue = BigInteger.ZERO;
		maxHilbertValue = TWO.pow(
				totalPrecision).subtract(
				BigDecimal.ONE).toBigInteger();
	}

	@Override
	public byte[] convertToHilbert(
			final double[] values,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {

		final List<BigInteger> dimensionValues = new ArrayList<BigInteger>();

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
			final List<BigInteger> values,
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

		compactHilbertCurve.index(
				bitVectors,
				0,
				hilbertBitVector);

		return hilbertBitVector;

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
	 *            bins expected bas on the cardinality of the definition
	 * @param isMin
	 *            flag indicating if this value is a minimum of a range in which
	 *            case it needs to be inclusive on a boundary, otherwise it is
	 *            exclusive
	 * @return value after normalization
	 * @throws IllegalArgumentException
	 *             thrown when the value passed doesn't fit with in the
	 *             dimension definition provided
	 */
	private BigInteger normalizeDimension(
			final SFCDimensionDefinition boundedDimensionDefinition,
			final double value,
			final BigDecimal bins,
			final boolean isMin,
			final boolean overInclusiveOnEdge )
			throws IllegalArgumentException {
		final double normalizedValue = boundedDimensionDefinition.normalize(value);
		if ((normalizedValue < 0) || (normalizedValue > 1)) {
			throw new IllegalArgumentException(
					"Value (" + value + ") is not within dimension bounds. The normalized value (" + normalizedValue
							+ ") must be within (0,1)");
		}
		final BigDecimal val = BigDecimal.valueOf(normalizedValue);
		// scale it to a value within the bits of precision
		final BigDecimal valueScaledWithinPrecision = val.multiply(bins);
		if ((isMin && !overInclusiveOnEdge) || (!isMin && overInclusiveOnEdge)) {
			// round it down, and make sure it isn't above bins - 1 (exactly 1
			// for the normalized value could produce a bit shifted value equal
			// to bins without this check)
			return valueScaledWithinPrecision.setScale(
					0,
					RoundingMode.FLOOR).min(
					bins.subtract(BigDecimal.ONE)).toBigInteger();
		}
		else {
			// round it up, subtract one to set the range between [0,
			// 2^cardinality-1)
			// and make sure it isn't below 0 (exactly 0 for the normalized
			// value
			// could produce a bit shifted value of -1 without this check)
			return valueScaledWithinPrecision.setScale(
					0,
					RoundingMode.CEILING).subtract(
					BigDecimal.ONE).max(
					BigDecimal.ZERO).toBigInteger();
		}

	}

	@Override
	public long[] indicesFromHilbert(
			final byte[] hilbertValue,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		// warning: this very much won't be unbounded because it returns an
		// array of longs right now
		// but we may as well re-use the calculation from the primitive
		// operations
		return PrimitiveHilbertSFCOperations.internalIndicesFromHilbert(
				hilbertValue,
				compactHilbertCurve,
				dimensionDefinitions);
	}

	@Override
	public MultiDimensionalNumericData convertFromHilbert(
			final byte[] hilbertValue,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final BitVector[] perDimensionBitVectors = PrimitiveHilbertSFCOperations.indexInverse(
				hilbertValue,
				compactHilbertCurve,
				dimensionDefinitions);
		final NumericRange[] retVal = new NumericRange[dimensionDefinitions.length];
		for (int i = 0; i < retVal.length; i++) {
			retVal[i] = denormalizeDimension(
					dimensionDefinitions[i],
					perDimensionBitVectors[i].toBigInteger(),
					binsPerDimension[i]);
		}
		return new BasicNumericDataset(
				retVal);
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
	 * @return range of values reprenenting this hilbert value (exlusive on the
	 *         end)
	 * @throws IllegalArgumentException
	 *             thrown when the value passed doesn't fit with in the hilbert
	 *             SFC for the dimension definition provided
	 */
	private NumericRange denormalizeDimension(
			final SFCDimensionDefinition boundedDimensionDefinition,
			final BigInteger value,
			final BigDecimal bins )
			throws IllegalArgumentException {
		final double min = new BigDecimal(
				value).divide(
				bins).doubleValue();
		final double max = new BigDecimal(
				value).add(
				BigDecimal.ONE).divide(
				bins).doubleValue();

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
		final List<BigInteger> minRangeList = new ArrayList<BigInteger>();
		final List<BigInteger> maxRangeList = new ArrayList<BigInteger>();

		final BigIntegerContent zero = new BigIntegerContent(
				BigInteger.valueOf(0L));
		final List<BigIntegerRange> region = new ArrayList<BigIntegerRange>(
				dimensionDefinitions.length);
		for (int d = 0; d < dimensionDefinitions.length; d++) {

			final BigInteger normalizedMin = normalizeDimension(
					dimensionDefinitions[d],
					rangePerDimension[d].getMin(),
					binsPerDimension[d],
					true,
					overInclusiveOnEdge);
			BigInteger normalizedMax = normalizeDimension(
					dimensionDefinitions[d],
					rangePerDimension[d].getMax(),
					binsPerDimension[d],
					false,
					overInclusiveOnEdge);
			if (normalizedMin.compareTo(normalizedMax) > 0) {
				// if they're both equal, which is possible because we treat max
				// as exclusive, set bin max to bin min (ie. treat it as
				// inclusive in this case)
				normalizedMax = normalizedMin;
			}
			minRangeList.add(normalizedMin);
			maxRangeList.add(normalizedMax);
			region.add(BigIntegerRange.of(
					normalizedMin,
					normalizedMax.add(BigInteger.ONE)));

		}

		final BigInteger minQuadSize = getMinimumQuadSize(
				minRangeList,
				maxRangeList);

		final RegionInspector<BigIntegerRange, BigIntegerContent> regionInspector = SimpleRegionInspector.create(
				ImmutableList.of(region),
				new BigIntegerContent(
						minQuadSize),
				Functions.<BigIntegerRange> identity(),
				BigIntegerRangeHome.INSTANCE,
				zero);

		final PlainFilterCombiner<BigIntegerRange, BigInteger, BigIntegerContent, BigIntegerRange> intervalCombiner = new PlainFilterCombiner<BigIntegerRange, BigInteger, BigIntegerContent, BigIntegerRange>(
				BigIntegerRange.of(
						0,
						1));

		final QueryBuilder<BigIntegerRange, BigIntegerRange> queryBuilder = BacktrackingQueryBuilder.create(
				regionInspector,
				intervalCombiner,
				maxFilteredIndexedRanges,
				removeVacuum,
				BigIntegerRangeHome.INSTANCE,
				zero);

		compactHilbertCurve.accept(new ZoomingSpaceVisitorAdapter(
				compactHilbertCurve,
				queryBuilder));

		// com.google.uzaygezen.core.Query<LongRange, LongRange> hilbertQuery =
		// queryBuilder.get();

		final List<FilteredIndexRange<BigIntegerRange, BigIntegerRange>> hilbertRanges = queryBuilder
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
			final FilteredIndexRange<BigIntegerRange, BigIntegerRange> range = hilbertRanges.get(i);
			// sanity check that values fit within the expected range
			// it seems that uzaygezen can produce a value at 2^totalPrecision
			// rather than 2^totalPrecision - 1
			final BigInteger startValue = clamp(
					minHilbertValue,
					maxHilbertValue,
					range.getIndexRange().getStart());
			final BigInteger endValue = clamp(
					minHilbertValue,
					maxHilbertValue,
					range.getIndexRange().getEnd().subtract(
							BigInteger.ONE));
			// make sure its padded if necessary
			final byte[] start = HilbertSFC.fitExpectedByteCount(
					expectedByteCount,
					startValue.toByteArray());

			// make sure its padded if necessary
			final byte[] end = HilbertSFC.fitExpectedByteCount(
					expectedByteCount,
					endValue.toByteArray());
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

	private static BigInteger clamp(
			final BigInteger minValue,
			final BigInteger maxValue,
			final BigInteger value ) {
		return value.max(
				minValue).min(
				maxValue);
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
	private BigInteger getMinimumQuadSize(
			final List<BigInteger> minRangeList,
			final List<BigInteger> maxRangeList ) {
		BigInteger maxRange = BigInteger.valueOf(1);
		final int dimensionality = Math.min(
				minRangeList.size(),
				maxRangeList.size());
		for (int d = 0; d < dimensionality; d++) {
			maxRange = maxRange.max(maxRangeList.get(
					d).subtract(
					minRangeList.get(d)).abs().add(
					BigInteger.ONE));
		}
		final BigInteger maxRangeDecomposed = maxRange.pow(dimensionality);
		if (maxRangeDecomposed.compareTo(UNIT_CELL_SIZE) <= 0) {
			return BigInteger.ONE;
		}

		return maxRangeDecomposed.divide(UNIT_CELL_SIZE);

	}

	@Override
	public BigInteger getEstimatedIdCount(
			final MultiDimensionalNumericData data,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final double[] mins = data.getMinValuesPerDimension();
		final double[] maxes = data.getMaxValuesPerDimension();
		BigInteger estimatedIdCount = BigInteger.valueOf(1);
		for (int d = 0; d < data.getDimensionCount(); d++) {
			final BigInteger binMin = normalizeDimension(
					dimensionDefinitions[d],
					mins[d],
					binsPerDimension[d],
					true,
					false);
			BigInteger binMax = normalizeDimension(
					dimensionDefinitions[d],
					maxes[d],
					binsPerDimension[d],
					false,
					false);
			if (binMin.compareTo(binMax) > 0) {
				// if they're both equal, which is possible because we treat max
				// as exclusive, set bin max to bin min (ie. treat it as
				// inclusive in this case)
				binMax = binMin;
			}
			estimatedIdCount = estimatedIdCount.multiply(binMax.subtract(
					binMin).abs().add(
					BigInteger.ONE));
		}
		return estimatedIdCount;
	}

	@Override
	public double[] getInsertionIdRangePerDimension(
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final double[] retVal = new double[dimensionDefinitions.length];
		for (int i = 0; i < dimensionDefinitions.length; i++) {
			retVal[i] = new BigDecimal(
					dimensionDefinitions[i].getRange()).divide(
					binsPerDimension[i]).doubleValue();
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
					true).longValue(),
			normalizeDimension(
					boundedDimensionDefinition,
					maxValue,
					binsPerDimension[dimension],
					false,
					true).longValue()
		};
	}
}
