package mil.nga.giat.geowave.core.index.sfc.tiered;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableBiMap.Builder;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.CoordinateRange;
import mil.nga.giat.geowave.core.index.FloatCompareUtils;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.binned.BinnedSFCUtils;
import mil.nga.giat.geowave.core.index.sfc.data.BinnedNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * This class uses multiple SpaceFillingCurve objects, one per tier, to
 * represent a single cohesive index strategy with multiple precisions
 *
 */
public class TieredSFCIndexStrategy implements
		HierarchicalNumericIndexStrategy
{
	private final static Logger LOGGER = LoggerFactory.getLogger(TieredSFCIndexStrategy.class);
	private final static int DEFAULT_MAX_ESTIMATED_DUPLICATE_IDS_PER_DIMENSION = 2;
	protected static final int DEFAULT_MAX_RANGES = -1;
	private SpaceFillingCurve[] orderedSfcs;
	private ImmutableBiMap<Integer, Byte> orderedSfcIndexToTierId;
	private NumericDimensionDefinition[] baseDefinitions;
	private long maxEstimatedDuplicateIdsPerDimension;
	private final Map<Integer, BigInteger> maxEstimatedDuplicatesPerDimensionalExtent = new HashMap<>();

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
		this(
				baseDefinitions,
				orderedSfcs,
				orderedSfcIndexToTierId,
				DEFAULT_MAX_ESTIMATED_DUPLICATE_IDS_PER_DIMENSION);
	}

	/**
	 * Constructor used to create a Tiered Index Strategy.
	 */
	public TieredSFCIndexStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final SpaceFillingCurve[] orderedSfcs,
			final ImmutableBiMap<Integer, Byte> orderedSfcIndexToTierId,
			final long maxEstimatedDuplicateIdsPerDimension ) {
		this.orderedSfcs = orderedSfcs;
		this.baseDefinitions = baseDefinitions;
		this.orderedSfcIndexToTierId = orderedSfcIndexToTierId;
		this.maxEstimatedDuplicateIdsPerDimension = maxEstimatedDuplicateIdsPerDimension;
		initDuplicateIdLookup();

	}

	private void initDuplicateIdLookup() {
		for (int i = 0; i <= baseDefinitions.length; i++) {
			final long maxEstimatedDuplicateIds = (long) Math.pow(
					maxEstimatedDuplicateIdsPerDimension,
					i);
			maxEstimatedDuplicatesPerDimensionalExtent.put(
					i,
					BigInteger.valueOf(maxEstimatedDuplicateIds));
		}
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxRangeDecomposition,
			final IndexMetaData... hints ) {
		// TODO don't just pass max ranges along to the SFC, take tiering and
		// binning into account to limit the number of ranges correctly

		final List<ByteArrayRange> queryRanges = new ArrayList<ByteArrayRange>();
		final BinnedNumericDataset[] binnedQueries = BinnedNumericDataset.applyBins(
				indexedRange,
				baseDefinitions);
		final TierIndexMetaData metaData = ((hints.length > 0) && (hints[0] != null) && (hints[0] instanceof TierIndexMetaData)) ? (TierIndexMetaData) hints[0]
				: null;

		for (int sfcIndex = orderedSfcs.length - 1; sfcIndex >= 0; sfcIndex--) {
			if ((metaData != null) && (metaData.tierCounts[sfcIndex] == 0)) {
				continue;
			}
			final SpaceFillingCurve sfc = orderedSfcs[sfcIndex];
			final Byte tier = orderedSfcIndexToTierId.get(sfcIndex);
			queryRanges.addAll(BinnedSFCUtils.getQueryRanges(
					binnedQueries,
					sfc,
					maxRangeDecomposition, // for now we're doing this
											// per SFC/tier rather than
											// dividing by the tiers
					tier));
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
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				DEFAULT_MAX_RANGES,
				hints);
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
				maxEstimatedDuplicatesPerDimensionalExtent.get(getRanges(indexedData)));
	}

	private static int getRanges(
			final MultiDimensionalNumericData indexedData ) {
		final double[] mins = indexedData.getMinValuesPerDimension();
		final double[] maxes = indexedData.getMaxValuesPerDimension();
		int ranges = 0;
		for (int d = 0; d < mins.length; d++) {
			if (!FloatCompareUtils.checkDoublesEqual(
					mins[d],
					maxes[d])) {
				ranges++;
			}
		}
		return ranges;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxDuplicateInsertionIdsPerDimension ) {
		return internalGetInsertionIds(
				indexedData,
				BigInteger.valueOf(maxDuplicateInsertionIdsPerDimension));
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
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		final byte[] rowId = insertionId.getBytes();
		if (rowId.length > 0) {
			final Integer orderedSfcIndex = orderedSfcIndexToTierId.inverse().get(
					rowId[0]);
			return new MultiDimensionalCoordinates(
					new byte[] {
						rowId[0]
					},
					BinnedSFCUtils.getCoordinatesForId(
							rowId,
							baseDefinitions,
							orderedSfcs[orderedSfcIndex]));
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
			return BinnedSFCUtils.getRangeForId(
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
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {
		final List<MultiDimensionalCoordinateRanges> coordRanges = new ArrayList<MultiDimensionalCoordinateRanges>();
		final BinRange[][] binRangesPerDimension = BinnedNumericDataset.getBinnedRangesPerDimension(
				dataRange,
				baseDefinitions);
		final TierIndexMetaData metaData = ((hints.length > 0) && (hints[0] != null) && (hints[0] instanceof TierIndexMetaData)) ? (TierIndexMetaData) hints[0]
				: null;

		for (int sfcIndex = orderedSfcs.length - 1; sfcIndex >= 0; sfcIndex--) {
			if ((metaData != null) && (metaData.tierCounts[sfcIndex] == 0)) {
				continue;
			}
			final SpaceFillingCurve sfc = orderedSfcs[sfcIndex];
			final Byte tier = orderedSfcIndexToTierId.get(sfcIndex);
			coordRanges.add(BinnedSFCUtils.getCoordinateRanges(
					binRangesPerDimension,
					sfc,
					baseDefinitions.length,
					tier));
		}
		return coordRanges.toArray(new MultiDimensionalCoordinateRanges[] {});
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(baseDefinitions);
		result = (prime * result)
				+ (int) (maxEstimatedDuplicateIdsPerDimension ^ (maxEstimatedDuplicateIdsPerDimension >>> 32));
		result = (prime * result) + ((orderedSfcIndexToTierId == null) ? 0 : orderedSfcIndexToTierId.hashCode());
		result = (prime * result) + Arrays.hashCode(orderedSfcs);
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
		final TieredSFCIndexStrategy other = (TieredSFCIndexStrategy) obj;
		if (!Arrays.equals(
				baseDefinitions,
				other.baseDefinitions)) {
			return false;
		}
		if (maxEstimatedDuplicateIdsPerDimension != other.maxEstimatedDuplicateIdsPerDimension) {
			return false;
		}
		if (orderedSfcIndexToTierId == null) {
			if (other.orderedSfcIndexToTierId != null) {
				return false;
			}
		}
		else if (!orderedSfcIndexToTierId.equals(other.orderedSfcIndexToTierId)) {
			return false;
		}
		if (!Arrays.equals(
				orderedSfcs,
				other.orderedSfcs)) {
			return false;
		}
		return true;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return baseDefinitions;
	}

	public boolean tierExists(
			Byte tierId ) {
		return orderedSfcIndexToTierId.containsValue(tierId);
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

		final BigInteger rowCount = sfc.getEstimatedIdCount(index);

		ByteArrayId singleId = BinnedSFCUtils.getSingleBinnedRowId(
				rowCount,
				tierId,
				index,
				sfc);
		if (singleId != null) {
			return Collections.singletonList(singleId);
		}

		if ((maxEstimatedDuplicateIds == null) || (rowCount.compareTo(maxEstimatedDuplicateIds) <= 0)
				|| (sfcIndex == 0)) {
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
		final RangeDecomposition rangeDecomp = sfc.decomposeRange(
				index,
				false,
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
					LOGGER
							.warn("Row IDs overflowed when ingesting data; start of range decomposition must be less than or equal to end of range. This may be because the start of the decomposed range is higher than the end of the range.");
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
		buf.putLong(maxEstimatedDuplicateIdsPerDimension);
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
		maxEstimatedDuplicateIdsPerDimension = buf.getLong();
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

		initDuplicateIdLookup();
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

	public void setMaxEstimatedDuplicateIdsPerDimension(
			final int maxEstimatedDuplicateIdsPerDimension ) {
		this.maxEstimatedDuplicateIdsPerDimension = maxEstimatedDuplicateIdsPerDimension;

		initDuplicateIdLookup();
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		final Set<ByteArrayId> retVal = new HashSet<ByteArrayId>(
				orderedSfcIndexToTierId.size());
		for (final Byte tier : orderedSfcIndexToTierId.values()) {
			retVal.add(new ByteArrayId(
					new byte[] {
						tier
					}));
		}
		return retVal;
	}

	@Override
	public int getByteOffsetFromDimensionalIndex() {
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
		return Collections.singletonList((IndexMetaData) new TierIndexMetaData(
				orderedSfcIndexToTierId.inverse()));
	}

	public static class TierIndexMetaData implements
			IndexMetaData
	{

		private int[] tierCounts = null;
		private ImmutableBiMap<Byte, Integer> orderedTierIdToSfcIndex = null;

		public TierIndexMetaData() {}

		public TierIndexMetaData(
				final ImmutableBiMap<Byte, Integer> orderedTierIdToSfcIndex ) {
			super();
			tierCounts = new int[orderedTierIdToSfcIndex.size()];
			this.orderedTierIdToSfcIndex = orderedTierIdToSfcIndex;
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buffer = ByteBuffer.allocate(4 + (tierCounts.length * 4));
			buffer.putInt(tierCounts.length);
			for (final int count : tierCounts) {
				buffer.putInt(count);
			}
			// do not use orderedTierIdToSfcIndex on query
			// for (final Entry<Byte,Integer > entry :
			// orderedTierIdToSfcIndex.entrySet()) {
			// buffer.put(entry.getKey().byteValue());
			// buffer.put(entry.getValue().byteValue());
			// }
			return buffer.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			tierCounts = new int[buffer.getInt()];
			for (int i = 0; i < tierCounts.length; i++) {
				tierCounts[i] = buffer.getInt();
			}
			// do not use orderedTierIdToSfcIndex on query
			// final Builder<Byte,Integer> bimapBuilder =
			// ImmutableBiMap.builder();
			// for (int i = 0; i < tierCounts.length; i++) {
			// bimapBuilder.put(
			// buffer.get(),
			// Byte.valueOf(buffer.get()).intValue()
			// );
			// }
			// orderedTierIdToSfcIndex = bimapBuilder.build();
		}

		@Override
		public void merge(
				final Mergeable merge ) {
			if (merge instanceof TierIndexMetaData) {
				final TierIndexMetaData other = (TierIndexMetaData) merge;
				int pos = 0;
				for (final int count : other.tierCounts) {
					tierCounts[pos++] += count;
				}
			}

		}

		@Override
		public void insertionIdsAdded(
				final List<ByteArrayId> ids ) {
			for (final ByteArrayId id : ids) {
				final byte first = id.getBytes()[0];
				if (orderedTierIdToSfcIndex.containsKey(first)) {
					tierCounts[orderedTierIdToSfcIndex.get(
							first).intValue()]++;
				}
			}
		}

		@Override
		public void insertionIdsRemoved(
				final List<ByteArrayId> ids ) {
			for (final ByteArrayId id : ids) {
				final byte first = id.getBytes()[0];
				if (orderedTierIdToSfcIndex.containsKey(first)) {
					tierCounts[orderedTierIdToSfcIndex.get(
							first).intValue()]--;
				}
			}
		}

		/**
		 * Convert Tiered Index Metadata statistics to a JSON object
		 */

		@Override
		public JSONObject toJSONObject()
				throws JSONException {
			JSONObject jo = new JSONObject();
			jo.put(
					"type",
					"TieredSFCIndexStrategy");

			jo.put(
					"TierCountsSize",
					tierCounts.length);

			if (null == orderedTierIdToSfcIndex) {
				jo.put(
						"orderedTierIdToSfcIndex",
						"null");
			}
			else {
				jo.put(
						"orderedTierIdToSfcIndexSize",
						orderedTierIdToSfcIndex.size());
			}

			return jo;
		}

	}
}
