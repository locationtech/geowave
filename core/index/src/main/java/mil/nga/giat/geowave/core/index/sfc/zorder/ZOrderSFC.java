package mil.nga.giat.geowave.core.index.sfc.zorder;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/***
 * Implementation of a ZOrder Space Filling Curve. Also called Morton, GeoHash,
 * etc.
 *
 */
public class ZOrderSFC implements
		SpaceFillingCurve
{
	private SFCDimensionDefinition[] dimensionDefs;
	private int cardinalityPerDimension;
	private double binsPerDimension;

	protected ZOrderSFC() {
		super();
	}

	/***
	 * Use the SFCFactory.createSpaceFillingCurve method - don't call this
	 * constructor directly
	 *
	 */
	public ZOrderSFC(
			final SFCDimensionDefinition[] dimensionDefs ) {
		init(dimensionDefs);
	}

	private void init(
			final SFCDimensionDefinition[] dimensionDefs ) {
		this.dimensionDefs = dimensionDefs;
		cardinalityPerDimension = 1;
		for (final SFCDimensionDefinition dimensionDef : dimensionDefs) {
			if (dimensionDef.getBitsOfPrecision() > cardinalityPerDimension) {
				cardinalityPerDimension = dimensionDef.getBitsOfPrecision();
			}
		}
		binsPerDimension = Math.pow(
				2,
				cardinalityPerDimension);
	}

	/***
	 * {@inheritDoc}
	 */
	@Override
	public byte[] getId(
			final double[] values ) {
		final double[] normalizedValues = new double[values.length];
		for (int d = 0; d < values.length; d++) {
			normalizedValues[d] = dimensionDefs[d].normalize(values[d]);
		}
		return ZOrderUtils.encode(
				normalizedValues,
				cardinalityPerDimension,
				values.length);
	}

	@Override
	public MultiDimensionalNumericData getRanges(
			final byte[] id ) {
		return new BasicNumericDataset(
				ZOrderUtils.decodeRanges(
						id,
						cardinalityPerDimension,
						dimensionDefs));
	}

	@Override
	public long[] getCoordinates(
			final byte[] id ) {
		return ZOrderUtils.decodeIndices(
				id,
				cardinalityPerDimension,
				dimensionDefs.length);
	}

	@Override
	public double[] getInsertionIdRangePerDimension() {
		final double[] retVal = new double[dimensionDefs.length];
		for (int i = 0; i < dimensionDefs.length; i++) {
			retVal[i] = dimensionDefs[i].getRange() / binsPerDimension;
		}
		return retVal;
	}

	@Override
	public BigInteger getEstimatedIdCount(
			final MultiDimensionalNumericData data ) {
		final double[] mins = data.getMinValuesPerDimension();
		final double[] maxes = data.getMaxValuesPerDimension();
		BigInteger estimatedIdCount = BigInteger.valueOf(1);
		for (int d = 0; d < data.getDimensionCount(); d++) {
			final double binMin = dimensionDefs[d].normalize(mins[d]) * binsPerDimension;
			final double binMax = dimensionDefs[d].normalize(maxes[d]) * binsPerDimension;
			estimatedIdCount = estimatedIdCount.multiply(BigInteger.valueOf((long) (Math.abs(binMax - binMin) + 1)));
		}
		return estimatedIdCount;
	}

	/***
	 * {@inheritDoc}
	 */
	@Override
	public RangeDecomposition decomposeRange(
			final MultiDimensionalNumericData query,
			final boolean overInclusiveOnEdge,
			final int maxFilteredIndexedRanges ) {
		// TODO: Because the research and benchmarking show Hilbert to
		// outperform Z-Order
		// the optimization of full query decomposition is not implemented at
		// the moment for Z-Order
		final double[] queryMins = query.getMinValuesPerDimension();
		final double[] queryMaxes = query.getMaxValuesPerDimension();
		final double[] normalizedMins = new double[query.getDimensionCount()];
		final double[] normalizedMaxes = new double[query.getDimensionCount()];
		for (int d = 0; d < query.getDimensionCount(); d++) {
			normalizedMins[d] = dimensionDefs[d].normalize(queryMins[d]);
			normalizedMaxes[d] = dimensionDefs[d].normalize(queryMaxes[d]);
		}
		final byte[] minZorder = ZOrderUtils.encode(
				normalizedMins,
				cardinalityPerDimension,
				query.getDimensionCount());
		final byte[] maxZorder = ZOrderUtils.encode(
				normalizedMaxes,
				cardinalityPerDimension,
				query.getDimensionCount());
		return new RangeDecomposition(
				new ByteArrayRange[] {
					new ByteArrayRange(
							new ByteArrayId(
									minZorder),
							new ByteArrayId(
									maxZorder))
				});
	}

	/***
	 * {@inheritDoc}
	 */
	@Override
	public RangeDecomposition decomposeRangeFully(
			final MultiDimensionalNumericData query ) {
		return decomposeRange(
				query,
				true,
				-1);
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
		init(dimensionDefs);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final String className = getClass().getName();
		result = (prime * result) + ((className == null) ? 0 : className.hashCode());
		result = (prime * result) + Arrays.hashCode(dimensionDefs);
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
		final ZOrderSFC other = (ZOrderSFC) obj;

		if (!Arrays.equals(
				dimensionDefs,
				other.dimensionDefs)) {
			return false;
		}
		return true;
	}

	@Override
	public long[] normalizeRange(
			final double minValue,
			final double maxValue,
			final int d ) {
		return new long[] {
			(long) (dimensionDefs[d].normalize(minValue) * binsPerDimension),
			(long) (dimensionDefs[d].normalize(maxValue) * binsPerDimension)
		};
	}
}
