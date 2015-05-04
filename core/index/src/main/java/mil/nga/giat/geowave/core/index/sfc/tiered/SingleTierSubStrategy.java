package mil.nga.giat.geowave.core.index.sfc.tiered;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.data.BinnedNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * This class wraps a single SpaceFillingCurve implementation with a tiered
 * approach to indexing (an SFC with a tier ID). This can be utilized by an
 * overall HierarchicalNumericIndexStrategy as an encapsulated sub-strategy.
 * 
 */
public class SingleTierSubStrategy implements
		NumericIndexStrategy
{
	private SpaceFillingCurve sfc;
	private NumericDimensionDefinition[] baseDefinitions;
	public byte tier;

	protected SingleTierSubStrategy() {}

	public SingleTierSubStrategy(
			final SpaceFillingCurve sfc,
			final NumericDimensionDefinition[] baseDefinitions,
			final byte tier ) {
		this.sfc = sfc;
		this.baseDefinitions = baseDefinitions;
		this.tier = tier;
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange ) {
		return getQueryRanges(
				indexedRange,
				TieredSFCIndexStrategy.DEFAULT_MAX_RANGES);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxRangeDecomposition ) {
		final BinnedNumericDataset[] binnedQueries = BinnedNumericDataset.applyBins(
				indexedRange,
				baseDefinitions);
		return TieredSFCIndexStrategy.getQueryRanges(
				binnedQueries,
				sfc,
				maxRangeDecomposition,
				tier);
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		final byte[] rowId = insertionId.getBytes();
		return TieredSFCIndexStrategy.getRangeForId(
				rowId,
				baseDefinitions,
				sfc);
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		final byte[] rowId = insertionId.getBytes();
		return TieredSFCIndexStrategy.getCoordinatesForId(
				rowId,
				baseDefinitions,
				sfc);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return getInsertionIds(
				indexedData,
				1);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxDuplicateInsertionIds ) {
		// we need to duplicate per bin so we can't adhere to max duplication
		// anyways
		final BinnedNumericDataset[] ranges = BinnedNumericDataset.applyBins(
				indexedData,
				baseDefinitions);
		// place each of these indices into a single row ID at a tier that will
		// fit its min and max
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
		for (final BinnedNumericDataset range : ranges) {
			final List<ByteArrayId> binRowIds = TieredSFCIndexStrategy.getRowIdsAtTier(
					range,
					tier,
					sfc,
					null,
					tier);
			if (binRowIds != null) {
				rowIds.addAll(binRowIds);
			}
		}
		return rowIds;
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return baseDefinitions;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(baseDefinitions);
		result = (prime * result) + ((sfc == null) ? 0 : sfc.hashCode());
		result = (prime * result) + tier;
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
		final SingleTierSubStrategy other = (SingleTierSubStrategy) obj;
		if (!Arrays.equals(
				baseDefinitions,
				other.baseDefinitions)) {
			return false;
		}
		if (sfc == null) {
			if (other.sfc != null) {
				return false;
			}
		}
		else if (!sfc.equals(other.sfc)) {
			return false;
		}
		if (tier != other.tier) {
			return false;
		}
		return true;
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 5;
		final List<byte[]> dimensionBinaries = new ArrayList<byte[]>(
				baseDefinitions.length);
		final byte[] sfcBinary = PersistenceUtils.toBinary(sfc);
		byteBufferLength += (4 + sfcBinary.length);
		for (final NumericDimensionDefinition dimension : baseDefinitions) {
			final byte[] dimensionBinary = PersistenceUtils.toBinary(dimension);
			byteBufferLength += (4 + dimensionBinary.length);
			dimensionBinaries.add(dimensionBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.put(tier);
		buf.putInt(baseDefinitions.length);
		buf.putInt(sfcBinary.length);
		buf.put(sfcBinary);
		for (final byte[] dimensionBinary : dimensionBinaries) {
			buf.putInt(dimensionBinary.length);
			buf.put(dimensionBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		tier = buf.get();
		final int numDimensions = buf.getInt();
		baseDefinitions = new NumericDimensionDefinition[numDimensions];
		final byte[] sfcBinary = new byte[buf.getInt()];
		buf.get(sfcBinary);
		sfc = PersistenceUtils.fromBinary(
				sfcBinary,
				SpaceFillingCurve.class);
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			baseDefinitions[i] = PersistenceUtils.fromBinary(
					dim,
					NumericDimensionDefinition.class);
		}
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return sfc.getInsertionIdRangePerDimension();
	}
}
