package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class HBaseSplitsProvider extends
		SplitsProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSplitsProvider.class);

	public static GeoWaveRowRange wrapRange(
			final ByteArrayRange range ) {
		return new HBaseRowRange(
				range);
	}

	public static ByteArrayRange unwrapRange(
			final GeoWaveRowRange range ) {
		if (range instanceof HBaseRowRange) {
			return ((HBaseRowRange) range).getRange();
		}
		LOGGER.error("HBaseSplitsProvider requires use of HBaseRowRange type.");
		return null;
	}

	@Override
	protected GeoWaveRowRange constructRange(
			final byte[] startKey,
			final boolean isStartKeyInclusive,
			final byte[] endKey,
			final boolean isEndKeyInclusive ) {
		return new HBaseRowRange(
				new ByteArrayRange(
						new ByteArrayId(
								startKey),
						new ByteArrayId(
								endKey)));
	}

	@Override
	protected GeoWaveRowRange defaultConstructRange() {
		return new HBaseRowRange();
	}

	@Override
	protected RangeLocationPair constructRangeLocationPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		return new HBaseRangeLocationPair(
				range,
				location,
				cardinality);
	}

	@Override
	public GeoWaveInputSplit constructInputSplit(
			final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo,
			final String[] locations ) {
		return new GeoWaveHBaseInputSplit(
				splitInfo,
				locations);
	}

	public static RangeLocationPair defaultConstructRangeLocationPair() {
		return new HBaseRangeLocationPair();
	}

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final DataStoreOperations operations,
			final PrimaryIndex index,
			final List<DataAdapter<Object>> adapters,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final DistributableQuery query,
			final String[] authorizations )
			throws IOException {

		BasicHBaseOperations hbaseOperations = null;
		if (operations instanceof BasicHBaseOperations) {
			hbaseOperations = (BasicHBaseOperations) operations;
		}
		else {
			LOGGER.error("HBaseSplitsProvider requires BasicHBaseOperations object.");
			return splits;
		}

		if ((query != null) && !query.isSupported(index)) {
			return splits;
		}
		final ByteArrayRange fullrange = unwrapRange(getRangeMax(
				index,
				adapterStore,
				statsStore,
				authorizations));

		final String tableName = index.getId().getString();
		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();

		// Build list of row ranges from query
		List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		final List<ByteArrayRange> constraintRanges;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(indexStrategy);
			if ((maxSplits != null) && (maxSplits > 0)) {
				constraintRanges = DataStoreUtils.constraintsToByteArrayRanges(
						indexConstraints,
						indexStrategy,
						maxSplits);
			}
			else {
				constraintRanges = DataStoreUtils.constraintsToByteArrayRanges(
						indexConstraints,
						indexStrategy,
						-1);
			}
			for (final ByteArrayRange constraintRange : constraintRanges) {
				ranges.add(constraintRange);
			}
		}
		else {
			ranges.add(fullrange);
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Protected range: " + fullrange);
			}
		}

		final Map<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> binnedRanges = new HashMap<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>>();
		final RegionLocator regionLocator = hbaseOperations.getRegionLocator(tableName);

		if (regionLocator == null) {
			LOGGER.error("Unable to retrieve RegionLocator for " + tableName);
			return splits;
		}

		while (!ranges.isEmpty()) {
			ranges = binRanges(
					ranges,
					binnedRanges,
					regionLocator);
		}

		for (final Entry<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> locationEntry : binnedRanges
				.entrySet()) {
			final String hostname = locationEntry.getKey().getHostname();

			for (final Entry<HRegionInfo, List<ByteArrayRange>> regionEntry : locationEntry.getValue().entrySet()) {
				final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo = new HashMap<PrimaryIndex, List<RangeLocationPair>>();
				final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();

				for (final ByteArrayRange range : regionEntry.getValue()) {
					GeoWaveRowRange rowRange = wrapRange(range);

					final double cardinality = getCardinality(
							getHistStats(
									index,
									adapters,
									adapterStore,
									statsStore,
									statsCache,
									authorizations),
							rowRange);

					if (range.intersects(fullrange)) {
						rangeList.add(constructRangeLocationPair(
								rowRange,
								hostname,
								cardinality < 1 ? 1.0 : cardinality));
					}
					else {
						LOGGER.info("Query split outside of range");
					}
					if (LOGGER.isTraceEnabled()) {
						LOGGER.warn("Clipped range: " + rangeList.get(
								rangeList.size() - 1).getRange());
					}
				}

				if (!rangeList.isEmpty()) {
					splitInfo.put(
							index,
							rangeList);
					splits.add(new IntermediateSplitInfo(
							splitInfo,
							this));
				}
			}
		}

		return splits;
	}

	private static List<ByteArrayRange> binRanges(
			final List<ByteArrayRange> inputRanges,
			final Map<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> binnedRanges,
			final RegionLocator regionLocator )
			throws IOException {

		// Loop through ranges, getting RegionLocation and RegionInfo for
		// startKey, clipping range by that regionInfo's extent, and leaving
		// remainder in the List to be region'd
		final ListIterator<ByteArrayRange> i = inputRanges.listIterator();
		while (i.hasNext()) {
			final ByteArrayRange range = i.next();

			final HRegionLocation location = regionLocator.getRegionLocation(range.getStart().getBytes());

			Map<HRegionInfo, List<ByteArrayRange>> regionInfoMap = binnedRanges.get(location);
			if (regionInfoMap == null) {
				regionInfoMap = new HashMap<HRegionInfo, List<ByteArrayRange>>();
				binnedRanges.put(
						location,
						regionInfoMap);
			}

			final HRegionInfo regionInfo = location.getRegionInfo();
			List<ByteArrayRange> rangeList = regionInfoMap.get(regionInfo);
			if (rangeList == null) {
				rangeList = new ArrayList<ByteArrayRange>();
				regionInfoMap.put(
						regionInfo,
						rangeList);
			}

			if (regionInfo.containsRange(
					range.getStart().getBytes(),
					range.getEnd().getBytes())) {
				rangeList.add(range);
				i.remove();
			}
			else {
				final ByteArrayRange overlappingRange = range.intersection(new ByteArrayRange(
						new ByteArrayId(
								regionInfo.getStartKey()),
						new ByteArrayId(
								regionInfo.getEndKey())));
				rangeList.add(overlappingRange);
				i.remove();

				final ByteArrayRange uncoveredRange = new ByteArrayRange(
						new ByteArrayId(
								regionInfo.getEndKey()),
						range.getEnd());

				i.add(uncoveredRange);
			}
		}

		return inputRanges;
	}
}
