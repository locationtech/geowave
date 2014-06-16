package mil.nga.giat.geowave.index.sfc.hilbert;

import java.math.BigInteger;
import java.nio.ByteBuffer;
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
			20);
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
			final List<Long> values,
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
	 *            bins expected based on the cardinality of the definition
	 * @return value after normalization
	 * @throws IllegalArgumentException
	 *             thrown when the value passed doesn't fit with in the
	 *             dimension definition provided
	 */
	private long normalizeDimension(
			final SFCDimensionDefinition boundedDimensionDefinition,
			final double value,
			final long bins )
			throws IllegalArgumentException {
		final double normalizedValue = boundedDimensionDefinition.normalize(value);
		if ((normalizedValue < 0) || (normalizedValue > 1)) {
			throw new IllegalArgumentException(
					"Value (" + value + ") is not within dimension bounds. The normalized value (" + normalizedValue + ") must be within (0,1)");
		}
		// scale it to a value within the bits of precision, and round up
		return (long) Math.max(
				Math.ceil(normalizedValue * bins) - 1L,
				0);

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
					binsPerDimension[d]);
			final long normalizedMax = normalizeDimension(
					dimensionDefinitions[d],
					rangePerDimension[d].getMax(),
					binsPerDimension[d]);
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

		compactHilbertCurve.accept(new ZoomingSpaceVisitorAdapter(
				compactHilbertCurve,
				queryBuilder));

		final List<FilteredIndexRange<LongRange, LongRange>> hilbertRanges = queryBuilder.get().getFilteredIndexRanges();

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
					range.getIndexRange().getEnd());
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
					binsPerDimension[d]);
			final long binMax = normalizeDimension(
					dimensionDefinitions[d],
					maxes[d],
					binsPerDimension[d]);
			estimatedIdCount *= (Math.abs(binMax - binMin) + 1);
		}
		return BigInteger.valueOf(estimatedIdCount);
	}
}
