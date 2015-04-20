package mil.nga.giat.geowave.core.index.sfc.hilbert;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

import com.google.uzaygezen.core.CompactHilbertCurve;
import com.google.uzaygezen.core.MultiDimensionalSpec;

/***
 * Implementation of a Compact Hilbert space filling curve
 * 
 */
public class HilbertSFC implements
		SpaceFillingCurve
{
	protected CompactHilbertCurve compactHilbertCurve;
	protected SFCDimensionDefinition[] dimensionDefinitions;
	protected int totalPrecision;

	/** Tunables **/
	private final static boolean REMOVE_VACUUM = true;
	protected HilbertSFCOperations getIdOperations;
	protected HilbertSFCOperations decomposeQueryOperations;

	protected HilbertSFC() {}

	/***
	 * Use the SFCFactory.createSpaceFillingCurve method - don't call this
	 * constructor directly
	 * 
	 */
	public HilbertSFC(
			final SFCDimensionDefinition[] dimensionDefs ) {
		init(dimensionDefs);
	}

	protected void init(
			final SFCDimensionDefinition[] dimensionDefs ) {

		final List<Integer> bitsPerDimension = new ArrayList<Integer>();
		totalPrecision = 0;
		for (final SFCDimensionDefinition dimension : dimensionDefs) {
			bitsPerDimension.add(dimension.getBitsOfPrecision());
			totalPrecision += dimension.getBitsOfPrecision();
		}

		compactHilbertCurve = new CompactHilbertCurve(
				new MultiDimensionalSpec(
						bitsPerDimension));

		dimensionDefinitions = dimensionDefs;
		setOptimalOperations(
				totalPrecision,
				bitsPerDimension,
				dimensionDefs);
	}

	protected void setOptimalOperations(
			final int totalPrecision,
			final List<Integer> bitsPerDimension,
			final SFCDimensionDefinition[] dimensionDefs ) {
		boolean primitiveForGetId = true;
		final boolean primitiveForQueryDecomposition = totalPrecision <= 62L;
		for (final Integer bits : bitsPerDimension) {
			if (bits > 48) {
				// if in any one dimension, more than 48 bits are used, we need
				// to use bigdecimals
				primitiveForGetId = false;
				break;
			}
		}
		if (primitiveForGetId) {
			final PrimitiveHilbertSFCOperations primitiveOps = new PrimitiveHilbertSFCOperations();
			primitiveOps.init(dimensionDefs);
			getIdOperations = primitiveOps;
			if (primitiveForQueryDecomposition) {
				decomposeQueryOperations = primitiveOps;
			}
			else {
				final UnboundedHilbertSFCOperations unboundedOps = new UnboundedHilbertSFCOperations();
				unboundedOps.init(dimensionDefs);
				decomposeQueryOperations = unboundedOps;
			}
		}
		else {
			final UnboundedHilbertSFCOperations unboundedOps = new UnboundedHilbertSFCOperations();
			unboundedOps.init(dimensionDefs);
			getIdOperations = unboundedOps;
			if (primitiveForQueryDecomposition) {
				final PrimitiveHilbertSFCOperations primitiveOps = new PrimitiveHilbertSFCOperations();
				primitiveOps.init(dimensionDefs);
				decomposeQueryOperations = primitiveOps;
			}
			else {
				decomposeQueryOperations = unboundedOps;
			}
		}
	}

	/***
	 * {@inheritDoc}
	 */
	@Override
	public byte[] getId(
			final double[] values ) {
		return getIdOperations.convertToHilbert(
				values,
				compactHilbertCurve,
				dimensionDefinitions);
	}

	/***
	 * {@inheritDoc}
	 */
	@Override
	public RangeDecomposition decomposeQueryFully(
			final MultiDimensionalNumericData query ) {
		return decomposeQuery(
				query,
				-1);
	}

	// TODO: improve this method - min/max not being calculated optimally
	/***
	 * {@inheritDoc}
	 */
	@Override
	public RangeDecomposition decomposeQuery(
			final MultiDimensionalNumericData query,
			int maxFilteredIndexedRanges ) {
		if (maxFilteredIndexedRanges == -1) {
			maxFilteredIndexedRanges = Integer.MAX_VALUE;
		}
		return decomposeQueryOperations.decomposeRange(
				query.getDataPerDimension(),
				compactHilbertCurve,
				dimensionDefinitions,
				totalPrecision,
				maxFilteredIndexedRanges,
				REMOVE_VACUUM);
	}

	protected static byte[] fitExpectedByteCount(
			final int expectedByteCount,
			final byte[] bytes ) {
		final int leftPadding = expectedByteCount - bytes.length;
		if (leftPadding > 0) {
			final byte[] zeroes = new byte[leftPadding];
			Arrays.fill(
					zeroes,
					(byte) 0);
			return ByteArrayUtils.combineArrays(
					zeroes,
					bytes);
		}
		else if (leftPadding < 0) {
			final byte[] truncatedBytes = new byte[expectedByteCount];

			if (bytes[0] != 0) {
				Arrays.fill(
						truncatedBytes,
						(byte) 255);
			}
			else {
				System.arraycopy(
						bytes,
						-leftPadding,
						truncatedBytes,
						0,
						expectedByteCount);
			}
			return truncatedBytes;
		}
		return bytes;
	}

	@Override
	public byte[] toBinary() {
		final List<byte[]> dimensionDefBinaries = new ArrayList<byte[]>(
				dimensionDefinitions.length);
		int bufferLength = 4;
		for (final SFCDimensionDefinition sfcDimension : dimensionDefinitions) {
			final byte[] sfcDimensionBinary = PersistenceUtils.toBinary(sfcDimension);
			bufferLength += (sfcDimensionBinary.length + 4);
			dimensionDefBinaries.add(sfcDimensionBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(bufferLength);
		buf.putInt(dimensionDefinitions.length);
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
		dimensionDefinitions = new SFCDimensionDefinition[numDimensions];
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			dimensionDefinitions[i] = PersistenceUtils.fromBinary(
					dim,
					SFCDimensionDefinition.class);
		}
		init(dimensionDefinitions);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final String className = getClass().getName();
		result = (prime * result) + ((className == null) ? 0 : className.hashCode());
		result = (prime * result) + Arrays.hashCode(dimensionDefinitions);
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
		final HilbertSFC other = (HilbertSFC) obj;

		if (!Arrays.equals(
				dimensionDefinitions,
				other.dimensionDefinitions)) {
			return false;
		}
		return true;
	}

	@Override
	public BigInteger getEstimatedIdCount(
			final MultiDimensionalNumericData data ) {
		return getIdOperations.getEstimatedIdCount(
				data,
				dimensionDefinitions);
	}

	@Override
	public int getBitsOfPrecision() {
		return totalPrecision;
	}

	@Override
	public MultiDimensionalNumericData getRanges(
			final byte[] id ) {
		return getIdOperations.convertFromHilbert(
				id,
				compactHilbertCurve,
				dimensionDefinitions);
	}

	@Override
	public long[] getCoordinates(
			final byte[] id ) {
		return getIdOperations.indicesFromHilbert(
				id,
				compactHilbertCurve,
				dimensionDefinitions);
	}

	@Override
	public double[] getInsertionIdRangePerDimension() {
		return getIdOperations.getInsertionIdRangePerDimension(dimensionDefinitions);
	}
}
