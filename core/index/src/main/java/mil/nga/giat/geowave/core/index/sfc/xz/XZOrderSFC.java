package mil.nga.giat.geowave.core.index.sfc.xz;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayRange.MergeOperation;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

public class XZOrderSFC implements
		SpaceFillingCurve
{
	private final static Logger LOGGER = LoggerFactory.getLogger(XZOrderSFC.class);
	private static double LOG_POINT_FIVE = Math.log(0.5);

	// the initial level of 2^dim tree
	private XElement[] LevelOneElements;

	// indicator that we have searched a full level of the 2^dim tree
	private XElement LevelTerminator;

	// TODO magic number; have to determine most appropriate value?
	private static int g = 12;

	private SFCDimensionDefinition[] dimensionDefs;
	private int dimensionCount;
	private int nthPowerOfTwo;

	public XZOrderSFC() {}

	public XZOrderSFC(
			SFCDimensionDefinition[] dimensionDefs ) {
		this.dimensionDefs = dimensionDefs;
		init();
	}

	private void init() {
		dimensionCount = dimensionDefs.length;
		nthPowerOfTwo = (int) Math.pow(
				2,
				dimensionCount);

		double[] mins = new double[dimensionCount];
		Arrays.fill(
				mins,
				0.0);
		double[] maxes = new double[dimensionCount];
		Arrays.fill(
				maxes,
				1.0);
		double[] negativeOnes = new double[dimensionCount];
		Arrays.fill(
				negativeOnes,
				-1.0);
		LevelOneElements = new XElement(
				mins,
				maxes,
				1.0).children();
		LevelTerminator = new XElement(
				negativeOnes,
				negativeOnes,
				0.0);
	}

	@Override
	public byte[] getId(
			double[] values ) {

		if (values.length == dimensionCount) {
			// We have a point, not a bounding box
			int boxCount = 0;
			double[] boxedValues = new double[dimensionCount * 2];
			for (int i = 0; i < dimensionCount; i++) {
				boxedValues[boxCount++] = values[i];
				boxedValues[boxCount++] = values[i];
			}
			values = boxedValues;
		}

		if (values.length != dimensionCount * 2) {
			LOGGER.error("Point or bounding box value count does not match number of indexed dimensions.");
			return null;
		}
		normalize(values);

		// calculate the length of the sequence code (section 4.1 of XZ-Ordering
		// paper)
		double maxDim = 0.0;
		for (int i = 0; (i + 1) < values.length; i++) {
			maxDim = Math.max(
					maxDim,
					Math.abs(values[i] - values[++i]));
		}

		// l1 (el-one) is a bit confusing to read, but corresponds with the
		// paper's definitions
		int l1 = (int) Math.floor(Math.log(maxDim) / LOG_POINT_FIVE);

		// the length will either be (l1) or (l1 + 1)
		int length = g;

		if (l1 < g) {
			double w2 = Math.pow(
					0.5,
					l1 + 1); // width of an element at
								// resolution l2 (l1 + 1)

			length = l1 + 1;
			for (int i = 0; (i + 1) < values.length; i++) {
				if (!predicate(
						values[i],
						values[++i],
						w2)) {
					length = l1;
					break;
				}
			}
		}

		double[] minValues = new double[values.length / 2];
		for (int i = 0; (i + 1) < values.length; i += 2) {
			minValues[i / 2] = values[i];
		}

		return sequenceCode(
				minValues,
				length);
	}

	// predicate for checking how many axis the polygon intersects
	// math.floor(min / w2) * w2 == start of cell containing min
	private boolean predicate(
			double min,
			double max,
			double w2 ) {
		return max <= (Math.floor(min / w2) * w2) + (2 * w2);
	}

	/**
	 * Normalize user space values to [0,1]
	 */
	private void normalize(
			double[] values ) {
		for (int i = 0; i < values.length; i++) {
			values[i] = dimensionDefs[i / 2].normalize(values[i]);
		}
	}

	private byte[] sequenceCode(
			double[] minValues,
			int length ) {

		double[] minsPerDimension = new double[dimensionCount];
		Arrays.fill(
				minsPerDimension,
				0.0);

		double[] maxesPerDimension = new double[dimensionCount];
		Arrays.fill(
				maxesPerDimension,
				1.0);

		long cs = 0L;

		for (int i = 0; i < length; i++) {

			double[] centers = new double[dimensionCount];
			for (int j = 0; j < dimensionCount; j++) {
				centers[j] = (minsPerDimension[j] + maxesPerDimension[j]) / 2.0;
			}

			BitSet bits = new BitSet(
					dimensionCount);
			for (int j = dimensionCount - 1; j >= 0; j--) {
				if (minValues[j] >= centers[j]) {
					bits.set(j);
				}
			}
			long bTerm = 0L;
			long[] longs = bits.toLongArray();
			if (longs.length > 0) {
				bTerm = longs[0];
			}

			cs += 1L + bTerm * (((long) (Math.pow(
					nthPowerOfTwo,
					g - i))) - 1L) / ((long) nthPowerOfTwo - 1);

			for (int j = 0; j < dimensionCount; j++) {
				if (minValues[j] < centers[j]) {
					maxesPerDimension[j] = centers[j];
				}
				else {
					minsPerDimension[j] = centers[j];
				}
			}
		}

		return ByteArrayUtils.longToByteArray(cs);
	}

	/**
	 * An extended Z curve element. Bounds refer to the non-extended z element
	 * for simplicity of calculation.
	 *
	 * An extended Z element refers to a normal Z curve element that has its
	 * upper bounds expanded by double its dimensions. By convention, an element
	 * is always an n-cube.
	 */
	private static class XElement
	{

		private final double[] minsPerDimension;
		private final double[] maxesPerDimension;
		private double length;

		private final Double[] extendedBounds;
		private XElement[] children;

		private final int dimensionCount;
		private final int nthPowerOfTwo;

		public XElement(
				double[] minsPerDimension,
				double[] maxesPerDimension,
				double length ) {
			this.minsPerDimension = minsPerDimension;
			this.maxesPerDimension = maxesPerDimension;
			this.length = length;
			dimensionCount = minsPerDimension.length;
			nthPowerOfTwo = (int) Math.pow(
					2,
					dimensionCount);
			extendedBounds = new Double[dimensionCount];
		}

		public XElement(
				XElement xElement ) {
			this(
					Arrays.copyOf(
							xElement.minsPerDimension,
							xElement.minsPerDimension.length),
					Arrays.copyOf(
							xElement.maxesPerDimension,
							xElement.maxesPerDimension.length),
					xElement.length);
		}

		// lazy-evaluated extended bounds
		public double getExtendedBound(
				int dimension ) {
			if (extendedBounds[dimension] == null) {
				extendedBounds[dimension] = maxesPerDimension[dimension] + length;
			}
			return extendedBounds[dimension];
		}

		public boolean isContained(
				final double[] windowMins,
				final double[] windowMaxes ) {
			for (int i = 0; i < dimensionCount; i++) {
				if (windowMins[i] > minsPerDimension[i] || windowMaxes[i] < getExtendedBound(i)) {
					return false;
				}
			}
			return true;
		}

		public boolean overlaps(
				final double[] windowMins,
				final double[] windowMaxes ) {
			for (int i = 0; i < dimensionCount; i++) {
				if (windowMaxes[i] < minsPerDimension[i] || windowMins[i] > getExtendedBound(i)) {
					return false;
				}
			}
			return true;
		}

		public XElement[] children() {
			if (children == null) {
				double[] centers = new double[dimensionCount];
				for (int i = 0; i < dimensionCount; i++) {
					centers[i] = (minsPerDimension[i] + maxesPerDimension[i]) / 2.0;
				}

				double len = length / 2.0;

				children = new XElement[nthPowerOfTwo];
				for (int i = 0; i < children.length; i++) {
					XElement child = new XElement(
							this);

					child.length = len;

					String binaryString = Integer.toBinaryString(i);
					// pad or trim binary as necessary to match dimensionality
					// of curve
					int paddingCount = binaryString.length() - dimensionCount;
					if (paddingCount > 0) {
						binaryString = binaryString.substring(paddingCount);
					}
					else {
						while (paddingCount < 0) {
							binaryString = "0" + binaryString;
							paddingCount++;
						}
					}

					for (int j = 1; j <= dimensionCount; j++) {
						if (binaryString.charAt(j - 1) == '1') {
							child.minsPerDimension[dimensionCount - j] = centers[dimensionCount - j];
						}
						else {
							child.maxesPerDimension[dimensionCount - j] = centers[dimensionCount - j];
						}
					}

					children[i] = child;
				}
			}

			return children;
		}
	}

	@Override
	public RangeDecomposition decomposeRangeFully(
			MultiDimensionalNumericData query ) {
		return decomposeRange(
				query,
				true,
				-1);
	}

	@Override
	public RangeDecomposition decomposeRange(
			MultiDimensionalNumericData query,
			boolean overInclusiveOnEdge,
			int maxRanges ) {

		// normalize query values
		double[] queryMins = query.getMinValuesPerDimension();
		double[] queryMaxes = query.getMaxValuesPerDimension();
		for (int i = 0; i < dimensionCount; i++) {
			queryMins[i] = dimensionDefs[i].normalize(queryMins[i]);
			queryMaxes[i] = dimensionDefs[i].normalize(queryMaxes[i]);
		}

		// stores our results - initial size of 100 in general saves us some
		// re-allocation
		ArrayList<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>(
				100);

		// values remaining to process - initial size of 100 in general saves us
		// some re-allocation
		ArrayDeque<XElement> remaining = new ArrayDeque<XElement>(
				100);

		// initial level
		for (XElement levelOneEl : LevelOneElements) {
			remaining.add(levelOneEl);
		}
		remaining.add(LevelTerminator);

		// level of recursion
		short level = 1;

		while (level < g && !remaining.isEmpty() && (maxRanges < 1 || ranges.size() < maxRanges)) {
			XElement next = remaining.poll();
			if (next.equals(LevelTerminator)) {
				// we've fully processed a level, increment our state
				if (!remaining.isEmpty()) {
					level = (short) (level + 1);
					remaining.add(LevelTerminator);
				}
			}
			else {
				checkValue(
						next,
						level,
						queryMins,
						queryMaxes,
						ranges,
						remaining);
			}
		}

		// bottom out and get all the ranges that partially overlapped but we
		// didn't fully process
		while (!remaining.isEmpty()) {
			XElement next = remaining.poll();
			if (next.equals(LevelTerminator)) {
				level = (short) (level + 1);
			}
			else {
				ByteArrayRange range = sequenceInterval(
						next.minsPerDimension,
						level,
						false);
				ranges.add(range);
			}
		}

		// we've got all our ranges - now reduce them down by merging
		// overlapping values
		// note: we don't bother reducing the ranges as in the XZ paper, as
		// accumulo handles lots of ranges fairly well
		ArrayList<ByteArrayRange> result = (ArrayList<ByteArrayRange>) ByteArrayRange.mergeIntersections(
				ranges,
				MergeOperation.UNION);

		return new RangeDecomposition(
				result.toArray(new ByteArrayRange[result.size()]));
	}

	// checks a single value and either:
	// eliminates it as out of bounds
	// adds it to our results as fully matching, or
	// adds it to our results as partial matching and queues up it's children
	// for further processing
	private void checkValue(
			XElement value,
			Short level,
			double[] queryMins,
			double[] queryMaxes,
			ArrayList<ByteArrayRange> ranges,
			ArrayDeque<XElement> remaining ) {
		if (value.isContained(
				queryMins,
				queryMaxes)) {
			// whole range matches, happy day
			ByteArrayRange range = sequenceInterval(
					value.minsPerDimension,
					level,
					false);
			ranges.add(range);
		}
		else if (value.overlaps(
				queryMins,
				queryMaxes)) {
			// some portion of this range is excluded
			// add the partial match and queue up each sub-range for processing
			ByteArrayRange range = sequenceInterval(
					value.minsPerDimension,
					level,
					true);
			ranges.add(range);
			for (XElement child : value.children()) {
				remaining.add(child);
			}
		}
	}

	/**
	 * Computes an interval of sequence codes for a given point - for polygons
	 * this is the lower-left corner.
	 *
	 * @param minsPerDimension
	 *            normalized min values [0,1] per dimension
	 * @param length
	 *            length of the sequence code that will used as the basis for
	 *            this interval
	 * @param partial
	 *            true if the element partially intersects the query window,
	 *            false if it is fully contained
	 * @return
	 */
	private ByteArrayRange sequenceInterval(
			double[] minsPerDimension,
			short length,
			boolean partial ) {
		byte[] min = sequenceCode(
				minsPerDimension,
				length);
		// if a partial match, we just use the single sequence code as an
		// interval
		// if a full match, we have to match all sequence codes starting with
		// the single sequence code
		byte[] max;
		if (partial) {
			max = min;
		}
		else {
			// from lemma 3 in the XZ-Ordering paper
			max = ByteArrayUtils.longToByteArray(ByteArrayUtils.byteArrayToLong(min) + (((long) (Math.pow(
					nthPowerOfTwo,
					g - length + 1))) - 1L) / ((long) (nthPowerOfTwo - 1)));
		}
		return new ByteArrayRange(
				new ByteArrayId(
						min),
				new ByteArrayId(
						max));
	}

	@Override
	public byte[] toBinary() {
		final List<byte[]> dimensionDefBinaries = new ArrayList<byte[]>(
				dimensionDefs.length);
		int bufferLength = 4;
		for (final SFCDimensionDefinition sfcDimension : dimensionDefs) {
			final byte[] sfcDimensionBinary = PersistenceUtils.toBinary(sfcDimension);
			bufferLength += (sfcDimensionBinary.length + 4);
			dimensionDefBinaries.add(sfcDimensionBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(bufferLength);
		buf.putInt(dimensionDefs.length);
		for (final byte[] dimensionDefBinary : dimensionDefBinaries) {
			buf.putInt(dimensionDefBinary.length);
			buf.put(dimensionDefBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numDimensions = buf.getInt();
		dimensionDefs = new SFCDimensionDefinition[numDimensions];
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			dimensionDefs[i] = PersistenceUtils.fromBinary(
					dim,
					SFCDimensionDefinition.class);
		}

		init();
	}

	@Override
	public double[] getInsertionIdRangePerDimension() {
		double normalizedSize = Math.pow(
				0.5,
				g);

		double[] rangesPerDimension = new double[dimensionCount];
		for (int i = 0; i < dimensionCount; i++) {
			rangesPerDimension[i] = dimensionDefs[i].denormalize(normalizedSize);
		}
		return rangesPerDimension;
	}

	@Override
	public BigInteger getEstimatedIdCount(
			MultiDimensionalNumericData data ) {
		// TODO Replace hard-coded value with real implementation?
		return BigInteger.ONE;
	}

	// TODO Backwords (sfc-space to user-space) conversion??
	@Override
	public MultiDimensionalNumericData getRanges(
			byte[] id ) {
		// use max range per dimension for now
		// to avoid false negatives
		NumericData[] dataPerDimension = new NumericData[dimensionCount];
		int i = 0;
		for (SFCDimensionDefinition dim : dimensionDefs) {
			dataPerDimension[i++] = dim.getFullRange();
		}
		return new BasicNumericDataset(
				dataPerDimension);
	}

	@Override
	public long[] getCoordinates(
			byte[] id ) {
		return null;
	}

	@Override
	public long[] normalizeRange(
			double minValue,
			double maxValue,
			int dimension ) {
		// TODO Auto-generated method stub
		return null;
	}

}
