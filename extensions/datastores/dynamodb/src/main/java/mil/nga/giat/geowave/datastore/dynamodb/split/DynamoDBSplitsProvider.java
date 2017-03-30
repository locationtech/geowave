package mil.nga.giat.geowave.datastore.dynamodb.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.log4j.Logger;

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
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.mapreduce.DynamoDBRangeLocatorPair;
import mil.nga.giat.geowave.datastore.dynamodb.mapreduce.DynamoDBRowRange;
import mil.nga.giat.geowave.datastore.dynamodb.mapreduce.GeoWaveDynamoDBInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class DynamoDBSplitsProvider extends
		SplitsProvider
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBSplitsProvider.class);

	public static GeoWaveRowRange wrapRange(
			final ByteArrayRange range ) {
		return new DynamoDBRowRange(
				range);
	}

	public static ByteArrayRange unwrapRange(
			final GeoWaveRowRange range ) {
		if (range instanceof DynamoDBRowRange) {
			return ((DynamoDBRowRange) range).getRange();
		}
		LOGGER.error("DynamoDBSplitsProvider requires use of DynamoDBRowRange type.");
		return null;
	}

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			TreeSet<IntermediateSplitInfo> splits,
			DataStoreOperations operations,
			PrimaryIndex left,
			List<DataAdapter<Object>> adapters,
			Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			AdapterStore adapterStore,
			DataStatisticsStore statsStore,
			Integer maxSplits,
			DistributableQuery query,
			String[] authorizations )
			throws IOException {

		DynamoDBOperations dynamoDBOperations = null;
		if (operations instanceof DynamoDBOperations) {
			dynamoDBOperations = (DynamoDBOperations) operations;
		}
		else {
			LOGGER.error("DynamoDBSplitsProvider requires DynamoDBOperations object.");
			return splits;
		}

		if ((query != null) && !query.isSupported(left)) {
			return splits;
		}

		// build a range around the values
		final ByteArrayRange fullrange = unwrapRange(getRangeMax(
				left,
				adapterStore,
				statsStore,
				authorizations));
		final String tableName = dynamoDBOperations.getQualifiedTableName(left.getId().getString());
		final NumericIndexStrategy indexStrategy = left.getIndexStrategy();

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

		final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo = new HashMap<PrimaryIndex, List<RangeLocationPair>>();
		final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();
		for (final ByteArrayRange range : ranges) {
			final double cardinality = getCardinality(
					getHistStats(
							left,
							adapters,
							adapterStore,
							statsStore,
							statsCache,
							authorizations),
					wrapRange(range));

			if (range.intersects(fullrange)) {
				rangeList.add(constructRangeLocationPair(
						wrapRange(range),
						tableName,
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
					left,
					rangeList);
			splits.add(new IntermediateSplitInfo(
					splitInfo,
					this));
		}

		return splits;
	}

	@Override
	protected GeoWaveRowRange constructRange(
			final byte[] startKey,
			final boolean isStartKeyInclusive,
			final byte[] endKey,
			final boolean isEndKeyInclusive ) {
		return new DynamoDBRowRange(
				new ByteArrayRange(
						new ByteArrayId(
								startKey),
						new ByteArrayId(
								endKey)));
	}

	@Override
	protected GeoWaveRowRange defaultConstructRange() {
		return new DynamoDBRowRange();
	}

	public static RangeLocationPair defaultConstructRangeLocationPair() {
		return new DynamoDBRangeLocatorPair();
	}

	@Override
	protected RangeLocationPair constructRangeLocationPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		return new DynamoDBRangeLocatorPair(
				range,
				location,
				cardinality);
	}

	@Override
	public GeoWaveInputSplit constructInputSplit(
			final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo,
			final String[] locations ) {
		return new GeoWaveDynamoDBInputSplit(
				splitInfo,
				locations);
	}

}
