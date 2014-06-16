package mil.nga.giat.geowave.index.sfc.hilbert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;

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
					20)).toBigInteger();
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
					"Number of dimensions supplied (" + values.length + ") is different than initialized (" + dimensionDefinitions.length + ").");
		}

		// Loop through each value, then normalize the value based on the
		// dimension definition
		for (int i = 0; i < dimensionDefinitions.length; i++) {
			dimensionValues.add(normalizeDimension(
					dimensionDefinitions[i],
					values[i],
					binsPerDimension[i]));
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

		final BitVector hilbertBitVector = BitVectorFactories.OPTIMAL.apply(compactHilbertCurve.getSpec().sumBitsPerDimension());

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
	 * @return value after normalization
	 * @throws IllegalArgumentException
	 *             thrown when the value passed doesn't fit with in the
	 *             dimension definition provided
	 */
	private BigInteger normalizeDimension(
			final SFCDimensionDefinition boundedDimensionDefinition,
			final double value,
			final BigDecimal bins )
			throws IllegalArgumentException {
		final double normalizedValue = boundedDimensionDefinition.normalize(value);
		if ((normalizedValue < 0) || (normalizedValue > 1)) {
			throw new IllegalArgumentException(
					"Value (" + value + ") is not within dimension bounds. The normalized value (" + normalizedValue + ") must be within (0,1)");
		}
		final BigDecimal val = BigDecimal.valueOf(normalizedValue);
		// scale it to a value within the bits of precision
		final BigDecimal valueScaledWithinPrecision = val.multiply(bins);
		// round it, subtract one to set the range between [0, 2^cardinality-1)
		// and make sure it isn't below 0 (exactly 0 for the normalized value
		// could produce a bit shifted value of -1 without this check)
		final BigInteger bitShiftedValue = valueScaledWithinPrecision.setScale(
				0,
				RoundingMode.CEILING).subtract(
				BigDecimal.ONE).max(
				BigDecimal.ZERO).toBigInteger();
		return bitShiftedValue;

	}

	@Override
	public RangeDecomposition decomposeRange(
			final NumericData[] rangePerDimension,
			final CompactHilbertCurve compactHilbertCurve,
			final SFCDimensionDefinition[] dimensionDefinitions,
			final int totalPrecision,
			final int maxFilteredIndexedRanges,
			final boolean removeVacuum ) {// List of query range minimum and
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
					binsPerDimension[d]);
			final BigInteger normalizedMax = normalizeDimension(
					dimensionDefinitions[d],
					rangePerDimension[d].getMax(),
					binsPerDimension[d]);
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

		final List<FilteredIndexRange<BigIntegerRange, BigIntegerRange>> hilbertRanges = queryBuilder.get().getFilteredIndexRanges();

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
					range.getIndexRange().getEnd());
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
					binsPerDimension[d]);
			final BigInteger binMax = normalizeDimension(
					dimensionDefinitions[d],
					maxes[d],
					binsPerDimension[d]);
			estimatedIdCount = estimatedIdCount.multiply(binMax.subtract(
					binMin).abs().add(
					BigInteger.ONE));
		}
		return estimatedIdCount;
	}
}
