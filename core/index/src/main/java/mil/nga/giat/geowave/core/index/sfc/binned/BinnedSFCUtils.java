package mil.nga.giat.geowave.core.index.sfc.binned;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Coordinate;
import mil.nga.giat.geowave.core.index.CoordinateRange;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.BinnedNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

public class BinnedSFCUtils
{

	public static List<ByteArrayRange> getQueryRanges(
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
			final RangeDecomposition rangeDecomp = sfc.decomposeRange(
					binnedQuery,
					true,
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

	public static MultiDimensionalCoordinateRanges getCoordinateRanges(
			final BinRange[][] binRangesPerDimension,
			final SpaceFillingCurve sfc,
			final int numDimensions,
			final byte tier ) {
		final CoordinateRange[][] coordinateRangesPerDimension = new CoordinateRange[numDimensions][];
		for (int d = 0; d < coordinateRangesPerDimension.length; d++) {
			coordinateRangesPerDimension[d] = new CoordinateRange[binRangesPerDimension[d].length];
			for (int i = 0; i < binRangesPerDimension[d].length; i++) {
				final long[] range = sfc.normalizeRange(
						binRangesPerDimension[d][i].getNormalizedMin(),
						binRangesPerDimension[d][i].getNormalizedMax(),
						d);
				coordinateRangesPerDimension[d][i] = new CoordinateRange(
						range[0],
						range[1],
						binRangesPerDimension[d][i].getBinId());
			}
		}
		return new MultiDimensionalCoordinateRanges(
				new byte[] {
					tier
				},
				coordinateRangesPerDimension);
	}

	public static ByteArrayId getSingleBinnedRowId(
			final BigInteger rowCount,
			final byte multiDimensionalId,
			final BinnedNumericDataset index,
			final SpaceFillingCurve sfc ) {
		if (rowCount.equals(BigInteger.ONE)) {
			final byte[] tierAndBinId = ByteArrayUtils.combineArrays(
					new byte[] {
						multiDimensionalId
					},
					index.getBinId());
			final double[] minValues = index.getMinValuesPerDimension();
			final double[] maxValues = index.getMaxValuesPerDimension();
			byte[] singleId = null;
			if (Arrays.equals(
					maxValues,
					minValues)) {
				singleId = sfc.getId(minValues);
			}
			else {
				byte[] minId = sfc.getId(minValues);
				byte[] maxId = sfc.getId(maxValues);

				if (Arrays.equals(
						minId,
						maxId)) {
					singleId = minId;
				}
			}
			if (singleId != null) {
				return new ByteArrayId(
						ByteArrayUtils.combineArrays(
								tierAndBinId,
								singleId));
			}
		}
		return null;
	}

	public static Coordinate[] getCoordinatesForId(
			final byte[] rowId,
			final NumericDimensionDefinition[] baseDefinitions,
			final SpaceFillingCurve sfc ) {
		final SFCIdAndBinInfo sfcIdAndBinInfo = getSFCIdAndBinInfo(
				rowId,
				baseDefinitions);
		final long[] coordinateValues = sfc.getCoordinates(sfcIdAndBinInfo.sfcId);
		final Coordinate[] retVal = new Coordinate[coordinateValues.length];
		for (int i = 0; i < coordinateValues.length; i++) {
			final byte[] bin = sfcIdAndBinInfo.binIds.get(i);
			retVal[i] = new Coordinate(
					coordinateValues[i],
					bin);
		}
		return retVal;
	}

	public static MultiDimensionalNumericData getRangeForId(
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
}
