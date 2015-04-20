package mil.nga.giat.geowave.core.index.sfc.tiered;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.BinnedNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableBiMap.Builder;

/**
 * This class uses multiple SpaceFillingCurve objects, one per tier, to
 * represent a single cohesive index strategy with multiple precisions
 * 
 */
public class TieredSFCIndexStrategy implements
		HierarchicalNumericIndexStrategy
{
	private final static Logger LOGGER = Logger.getLogger(TieredSFCIndexStrategy.class);
	private final static int MAX_ESTIMATED_DUPLICATE_IDS_PER_DIMENSION = 2;
	protected static final int DEFAULT_MAX_RANGES = -1;
	private SpaceFillingCurve[] orderedSfcs;
	private ImmutableBiMap<Integer, Byte> orderedSfcIndexToTierId;
	private NumericDimensionDefinition[] baseDefinitions;
	private long maxEstimatedDuplicateIds;
	private BigInteger maxEstimatedDuplicateIdsBigInteger;

	protected TieredSFCIndexStrategy() {}

	/**
	 * Constructor used to create a Tiered Index Strategy.
	 * 
	 * @param baseDefinitions
	 *            the dimension definitions of the space filling curve
	 * @param orderedSfcs
	 *            the space filling curve used to create the strategy
	 */
	public TieredSFCIndexStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final SpaceFillingCurve[] orderedSfcs,
			final ImmutableBiMap<Integer, Byte> orderedSfcIndexToTierId ) {
		this.orderedSfcs = orderedSfcs;
		this.baseDefinitions = baseDefinitions;
		this.orderedSfcIndexToTierId = orderedSfcIndexToTierId;
		maxEstimatedDuplicateIds = (long) Math.pow(
				MAX_ESTIMATED_DUPLICATE_IDS_PER_DIMENSION,
				baseDefinitions.length);
		maxEstimatedDuplicateIdsBigInteger = BigInteger.valueOf(maxEstimatedDuplicateIds);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxRangeDecomposition ) {
		// TODO don't just pass max ranges along to the SFC, take tiering and
		// binning into account to limit the number of ranges correctly

		final List<ByteArrayRange> queryRanges = new ArrayList<ByteArrayRange>();
		final BinnedNumericDataset[] binnedQueries = BinnedNumericDataset.applyBins(
				indexedRange,
				baseDefinitions);
		int maxRangeDecompositionPerSfc = maxRangeDecomposition;
		if ((maxRangeDecomposition > 1) && (orderedSfcs.length > 1)) {
			maxRangeDecompositionPerSfc = (int) Math.ceil((double) maxRangeDecomposition / (double) orderedSfcs.length);
		}
		for (int sfcIndex = orderedSfcs.length - 1; sfcIndex >= 0; sfcIndex--) {
			final SpaceFillingCurve sfc = orderedSfcs[sfcIndex];
			final Byte tier = orderedSfcIndexToTierId.get(sfcIndex);
			queryRanges.addAll(getQueryRanges(
					binnedQueries,
					sfc,
					maxRangeDecompositionPerSfc,
					tier));
		}
		return queryRanges;
	}

	protected static List<ByteArrayRange> getQueryRanges(
			final BinnedNumericDataset[] binnedQueries,
			final SpaceFillingCurve sfc,
			final int maxRanges,
			final byte tier ) {
		final List<ByteArrayRange> queryRanges = new ArrayList<ByteArrayRange>();

		int maxRangeDecompositionPerBin = maxRanges;
		if ((maxRanges > 1) && (binnedQueries.length > 1)) {
			maxRangeDecompositionPerBin = (int) Math.ceil((double) maxRanges / (double) binnedQueries.length);
		}
		for (final BinnedNumericDataset binnedQuery : binnedQueries) {
			final RangeDecomposition rangeDecomp = sfc.decomposeQuery(
					binnedQuery,
					maxRangeDecompositionPerBin);
			final byte[] tierAndBinId = ByteArrayUtils.combineArrays(
					new byte[] {
						tier
					// we're assuming tiers only go to 127 (the max byte
					// value)
					},
					binnedQuery.getBinId());
			for (final ByteArrayRange range : rangeDecomp.getRanges()) {
				queryRanges.add(new ByteArrayRange(
						new ByteArrayId(
								ByteArrayUtils.combineArrays(
										tierAndBinId,
										range.getStart().getBytes())),
						new ByteArrayId(
								ByteArrayUtils.combineArrays(
										tierAndBinId,
										range.getEnd().getBytes()))));
			}
		}
		return queryRanges;
	}

	/**
	 * Returns a list of query ranges for an specified numeric range.
	 * 
	 * @param indexedRange
	 *            defines the numeric range for the query
	 * @return a List of query ranges
	 */
	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange ) {
		return getQueryRanges(
				indexedRange,
				DEFAULT_MAX_RANGES);
	}

	/**
	 * Returns a list of id's for insertion.
	 * 
	 * @param indexedData
	 *            defines the numeric data to be indexed
	 * @return a List of insertion ID's
	 */
	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return internalGetInsertionIds(
				indexedData,
				maxEstimatedDuplicateIdsBigInteger);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxDuplicateInsertionIds ) {
		return internalGetInsertionIds(
				indexedData,
				BigInteger.valueOf(maxDuplicateInsertionIds));
	}

	private List<ByteArrayId> internalGetInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final BigInteger maxDuplicateInsertionIds ) {
		final BinnedNumericDataset[] ranges = BinnedNumericDataset.applyBins(
				indexedData,
				baseDefinitions);
		// place each of these indices into a single row ID at a tier that will
		// fit its min and max
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				ranges.length);
		for (final BinnedNumericDataset range : ranges) {
			rowIds.addAll(getRowIds(
					range,
					maxDuplicateInsertionIds));
		}
		return rowIds;
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		final byte[] rowId = insertionId.getBytes();
		if (rowId.length > 0) {
			final Integer orderedSfcIndex = orderedSfcIndexToTierId.inverse().get(
					rowId[0]);
			return getCoordinatesForId(
					rowId,
					baseDefinitions,
					orderedSfcs[orderedSfcIndex]);
		}
		else {
			LOGGER.warn("Row must at least contain a byte for tier");
		}
		return null;
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		final byte[] rowId = insertionId.getBytes();
		if (rowId.length > 0) {
			final Integer orderedSfcIndex = orderedSfcIndexToTierId.inverse().get(
					rowId[0]);
			return getRangeForId(
					rowId,
					baseDefinitions,
					orderedSfcs[orderedSfcIndex]);
		}
		else {
			LOGGER.warn("Row must at least contain a byte for tier");
		}
		return null;
	}

	protected static long[] getCoordinatesForId(
			final byte[] rowId,
			final NumericDimensionDefinition[] baseDefinitions,
			final SpaceFillingCurve sfc ) {
		final SFCIdAndBinInfo sfcIdAndBinInfo = getSFCIdAndBinInfo(
				rowId,
				baseDefinitions);
		return sfc.getCoordinates(sfcIdAndBinInfo.sfcId);
	}

	protected static MultiDimensionalNumericData getRangeForId(
			final byte[] rowId,
			final NumericDimensionDefinition[] baseDefinitions,
			final SpaceFillingCurve sfc ) {
		final SFCIdAndBinInfo sfcIdAndBinInfo = getSFCIdAndBinInfo(
				rowId,
				baseDefinitions);
		final MultiDimensionalNumericData numericData = sfc.getRanges(sfcIdAndBinInfo.sfcId);
		// now we need to unapply the bins to the data, denormalizing the
		// ranges to the native bounds
		if (sfcIdAndBinInfo.rowIdOffset > 1) {
			final NumericData[] data = numericData.getDataPerDimension();
			for (final Entry<Integer, byte[]> entry : sfcIdAndBinInfo.binIds.entrySet()) {
				final int dimension = entry.getKey();
				final NumericRange range = baseDefinitions[dimension].getDenormalizedRange(new BinRange(
						entry.getValue(),
						data[dimension].getMin(),
						data[dimension].getMax(),
						false));
				data[dimension] = range;
			}
			return new BasicNumericDataset(
					data);
		}
		return numericData;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return baseDefinitions;
	}

	synchronized private List<ByteArrayId> getRowIds(
			final BinnedNumericDataset index,
			final BigInteger maxEstimatedDuplicateIds ) {
		// most times this should be a single row ID, but if the lowest
		// precision tier does not have a single SFC value for this data, it
		// will be multiple row IDs

		// what tier does this entry belong in?
		for (int sfcIndex = orderedSfcs.length - 1; sfcIndex >= 0; sfcIndex--) {
			final SpaceFillingCurve sfc = orderedSfcs[sfcIndex];
			// loop through space filling curves and stop when both the min and
			// max of the ranges fit the same row ID
			final byte tierId = orderedSfcIndexToTierId.get(sfcIndex);
			final List<ByteArrayId> rowIdsAtTier = getRowIdsAtTier(
					index,
					tierId,
					sfc,
					maxEstimatedDuplicateIds,
					sfcIndex);
			if (rowIdsAtTier != null) {
				return rowIdsAtTier;
			}
		}

		// this should never happen because of the check for tier 0
		return new ArrayList<ByteArrayId>();
	}

	protected static List<ByteArrayId> getRowIdsAtTier(
			final BinnedNumericDataset index,
			final byte tierId,
			final SpaceFillingCurve sfc,
			final BigInteger maxEstimatedDuplicateIds,
			final int sfcIndex ) {
		final List<ByteArrayId> retVal = new ArrayList<ByteArrayId>();
		final BigInteger rowCount = sfc.getEstimatedIdCount(index);
		if (rowCount.equals(BigInteger.ONE)) {
			final byte[] tierAndBinId = ByteArrayUtils.combineArrays(
					new byte[] {
						tierId
					},
					index.getBinId());
			final double[] maxValues = index.getMaxValuesPerDimension();
			retVal.add(new ByteArrayId(
					ByteArrayUtils.combineArrays(
							tierAndBinId,
							sfc.getId(maxValues))));
			return retVal;
		}
		else if ((maxEstimatedDuplicateIds == null) || (rowCount.compareTo(maxEstimatedDuplicateIds) <= 0) || (sfcIndex == 0)) {
			return decomposeRangesForEntry(
					index,
					tierId,
					sfc);
		}
		return null;
	}

	protected static List<ByteArrayId> decomposeRangesForEntry(
			final BinnedNumericDataset index,
			final byte tierId,
			final SpaceFillingCurve sfc ) {
		final List<ByteArrayId> retVal = new ArrayList<ByteArrayId>();
		final byte[] tierAndBinId = ByteArrayUtils.combineArrays(
				new byte[] {
					tierId
				},
				index.getBinId());
		final RangeDecomposition rangeDecomp = sfc.decomposeQuery(
				index,
				DEFAULT_MAX_RANGES);
		// this range does not fit into a single row ID at the lowest
		// tier, decompose it
		for (final ByteArrayRange range : rangeDecomp.getRanges()) {
			final byte[] currentRowId = Arrays.copyOf(
					range.getStart().getBytes(),
					range.getStart().getBytes().length);
			retVal.add(new ByteArrayId(
					ByteArrayUtils.combineArrays(
							tierAndBinId,
							currentRowId)));
			while (!Arrays.equals(
					currentRowId,
					range.getEnd().getBytes())) {
				// increment until we reach the end row ID
				boolean overflow = !ByteArrayUtils.increment(currentRowId);
				if (!overflow) {
					retVal.add(new ByteArrayId(
							ByteArrayUtils.combineArrays(
									tierAndBinId,
									currentRowId)));
				}
				else {
					// the increment caused an overflow which shouldn't
					// ever happen assuming the start row ID is less
					// than the end row ID
					LOGGER.warn("Row IDs overflowed when ingesting data; start of range decomposition must be less than or equal to end of range. This may be because the start of the decomposed range is higher than the end of the range.");
					overflow = true;
					break;
				}
			}
		}
		return retVal;
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 20 + (2 * orderedSfcIndexToTierId.size());
		final List<byte[]> orderedSfcBinaries = new ArrayList<byte[]>(
				orderedSfcs.length);
		final List<byte[]> dimensionBinaries = new ArrayList<byte[]>(
				baseDefinitions.length);
		for (final SpaceFillingCurve sfc : orderedSfcs) {
			final byte[] sfcBinary = PersistenceUtils.toBinary(sfc);
			byteBufferLength += (4 + sfcBinary.length);
			orderedSfcBinaries.add(sfcBinary);
		}
		for (final NumericDimensionDefinition dimension : baseDefinitions) {
			final byte[] dimensionBinary = PersistenceUtils.toBinary(dimension);
			byteBufferLength += (4 + dimensionBinary.length);
			dimensionBinaries.add(dimensionBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.putInt(orderedSfcs.length);
		buf.putInt(baseDefinitions.length);
		buf.putInt(orderedSfcIndexToTierId.size());
		buf.putLong(maxEstimatedDuplicateIds);
		for (final byte[] sfcBinary : orderedSfcBinaries) {
			buf.putInt(sfcBinary.length);
			buf.put(sfcBinary);
		}
		for (final byte[] dimensionBinary : dimensionBinaries) {
			buf.putInt(dimensionBinary.length);
			buf.put(dimensionBinary);
		}
		for (final Entry<Integer, Byte> entry : orderedSfcIndexToTierId.entrySet()) {
			buf.put(entry.getKey().byteValue());
			buf.put(entry.getValue());
		}

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numSfcs = buf.getInt();
		final int numDimensions = buf.getInt();
		final int mappingSize = buf.getInt();
		maxEstimatedDuplicateIds = buf.getLong();
		maxEstimatedDuplicateIdsBigInteger = BigInteger.valueOf(maxEstimatedDuplicateIds);
		orderedSfcs = new SpaceFillingCurve[numSfcs];
		baseDefinitions = new NumericDimensionDefinition[numDimensions];
		for (int i = 0; i < numSfcs; i++) {
			final byte[] sfc = new byte[buf.getInt()];
			buf.get(sfc);
			orderedSfcs[i] = PersistenceUtils.fromBinary(
					sfc,
					SpaceFillingCurve.class);
		}
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			baseDefinitions[i] = PersistenceUtils.fromBinary(
					dim,
					NumericDimensionDefinition.class);
		}
		final Builder<Integer, Byte> bimapBuilder = ImmutableBiMap.builder();
		for (int i = 0; i < mappingSize; i++) {
			bimapBuilder.put(
					Byte.valueOf(
							buf.get()).intValue(),
					buf.get());
		}
		orderedSfcIndexToTierId = bimapBuilder.build();
	}

	@Override
	public SubStrategy[] getSubStrategies() {
		final SubStrategy[] subStrategies = new SubStrategy[orderedSfcs.length];
		for (int sfcIndex = 0; sfcIndex < orderedSfcs.length; sfcIndex++) {
			final byte tierId = orderedSfcIndexToTierId.get(sfcIndex);
			subStrategies[sfcIndex] = new SubStrategy(
					new SingleTierSubStrategy(
							orderedSfcs[sfcIndex],
							baseDefinitions,
							tierId),
					new byte[] {
						tierId
					});
		}
		return subStrategies;
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		// delegate this to the highest precision tier SFC
		return orderedSfcs[orderedSfcs.length - 1].getInsertionIdRangePerDimension();
	}

	private static SFCIdAndBinInfo getSFCIdAndBinInfo(
			final byte[] rowId,
			final NumericDimensionDefinition[] baseDefinitions ) {

		final Map<Integer, byte[]> binIds = new HashMap<Integer, byte[]>();
		// one for the tier
		int rowIdOffset = 1;
		for (int dimensionIdx = 0; dimensionIdx < baseDefinitions.length; dimensionIdx++) {
			final int binSize = baseDefinitions[dimensionIdx].getFixedBinIdSize();
			if (binSize > 0) {
				binIds.put(
						dimensionIdx,
						Arrays.copyOfRange(
								rowId,
								rowIdOffset,
								rowIdOffset + binSize));
				rowIdOffset += binSize;
			}
		}
		final byte[] sfcId = Arrays.copyOfRange(
				rowId,
				rowIdOffset,
				rowId.length);
		return new SFCIdAndBinInfo(
				sfcId,
				binIds,
				rowIdOffset);
	}

	private static class SFCIdAndBinInfo
	{
		private final byte[] sfcId;
		private final Map<Integer, byte[]> binIds;
		private final int rowIdOffset;

		public SFCIdAndBinInfo(
				final byte[] sfcId,
				final Map<Integer, byte[]> binIds,
				final int rowIdOffset ) {
			super();
			this.sfcId = sfcId;
			this.binIds = binIds;
			this.rowIdOffset = rowIdOffset;
		}
	}

	public void setMaxEstimatedDuplicateIds(
			int maxEstimatedDuplicateIds ) {
		this.maxEstimatedDuplicateIds = maxEstimatedDuplicateIds;
		maxEstimatedDuplicateIdsBigInteger = BigInteger.valueOf(maxEstimatedDuplicateIds);
	}
}
