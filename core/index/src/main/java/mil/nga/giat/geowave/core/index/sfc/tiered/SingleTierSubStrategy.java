package mil.nga.giat.geowave.core.index.sfc.tiered;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
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
	private final static Logger LOGGER = Logger.getLogger(SingleTierSubStrategy.class);
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
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				TieredSFCIndexStrategy.DEFAULT_MAX_RANGES);
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxRangeDecomposition,
			final IndexMetaData... hints ) {
		final BinnedNumericDataset[] binnedQueries = BinnedNumericDataset.applyBins(
				indexedRange,
				baseDefinitions);
		return new QueryRanges(
				TieredSFCIndexStrategy.getQueryRanges(
						binnedQueries,
						sfc,
						maxRangeDecomposition,
						tier));
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		final List<ByteArrayId> insertionIds = new SinglePartitionInsertionIds(
				partitionKey,
				sortKey).getCompositeInsertionIds();
		if (insertionIds.isEmpty()) {
			LOGGER.warn("Unexpected empty insertion ID in getRangeForId()");
			return null;
		}
		final byte[] rowId = insertionIds.get(
				0).getBytes();
		return TieredSFCIndexStrategy.getRangeForId(
				rowId,
				baseDefinitions,
				sfc);
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		final byte[] rowId = ByteArrayUtils.combineArrays(
				partitionKey == null ? null : partitionKey.getBytes(),
				sortKey == null ? null : sortKey.getBytes());
		return new MultiDimensionalCoordinates(
				new byte[] {
					tier
				},
				TieredSFCIndexStrategy.getCoordinatesForId(
						rowId,
						baseDefinitions,
						sfc));
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return getInsertionIds(
				indexedData,
				1);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxDuplicateInsertionIds ) {
		// we need to duplicate per bin so we can't adhere to max duplication
		// anyways
		final BinnedNumericDataset[] ranges = BinnedNumericDataset.applyBins(
				indexedData,
				baseDefinitions);
		final Set<SinglePartitionInsertionIds> retVal = new HashSet<SinglePartitionInsertionIds>(
				ranges.length);
		for (final BinnedNumericDataset range : ranges) {
			final SinglePartitionInsertionIds binRowIds = TieredSFCIndexStrategy.getRowIdsAtTier(
					range,
					tier,
					sfc,
					null,
					tier);
			if (binRowIds != null) {
				retVal.add(binRowIds);
			}
		}
		return new InsertionIds(
				retVal);
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

	@Override
	public int getPartitionKeyLength() {
		int rowIdOffset = 1;
		for (int dimensionIdx = 0; dimensionIdx < baseDefinitions.length; dimensionIdx++) {
			final int binSize = baseDefinitions[dimensionIdx].getFixedBinIdSize();
			if (binSize > 0) {
				rowIdOffset += binSize;
			}
		}
		return rowIdOffset;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.<IndexMetaData> emptyList();
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {
		final BinRange[][] binRangesPerDimension = BinnedNumericDataset.getBinnedRangesPerDimension(
				dataRange,
				baseDefinitions);
		return new MultiDimensionalCoordinateRanges[] {
			TieredSFCIndexStrategy.getCoordinateRanges(
					binRangesPerDimension,
					sfc,
					baseDefinitions.length,
					tier)
		};
	}

	@Override
	public Set<ByteArrayId> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		return TieredSFCIndexStrategy.internalGetInsertionKeys(
				this,
				insertionData);
	}

	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		return TieredSFCIndexStrategy.internalGetQueryPartitionKeys(
				this,
				queryData,
				hints);
	}
}
