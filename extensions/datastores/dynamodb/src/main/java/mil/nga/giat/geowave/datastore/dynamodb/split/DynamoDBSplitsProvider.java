package mil.nga.giat.geowave.datastore.dynamodb.split;

import java.io.IOException;
import java.util.ArrayList;
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
import mil.nga.giat.geowave.datastore.dynamodb.query.DynamoDBConstraintsQuery;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class DynamoDBSplitsProvider extends SplitsProvider {
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
		LOGGER.error("HBaseSplitsProvider requires use of HBaseRowRange type.");
		return null;
	}

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(TreeSet<IntermediateSplitInfo> splits,
			DataStoreOperations operations, PrimaryIndex left, List<DataAdapter<Object>> value,
			Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache, AdapterStore adapterStore,
			DataStatisticsStore statsStore, Integer maxSplits, DistributableQuery query, String[] authorizations)
			throws IOException {
		
		DynamoDBOperations dynamoDBOperations = null;
		if (operations instanceof DynamoDBOperations) {
			dynamoDBOperations = (DynamoDBOperations) operations;
		}
		else {
			LOGGER.error("AccumuloSplitsProvider requires AccumuloOperations object.");
			return splits;
		}

		if ((query != null) && !query.isSupported(left)) {
			return splits;
		}
		
		//build a range around the values 
		final ByteArrayRange fullrange = unwrapRange(getRangeMax(
			left,
			adapterStore,
			statsStore,
			authorizations));
		
		final String tableName = left.getId().getString();
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
		
//		new DynamoDBConstraintsQuery(
//			dataStore, 
//			dynamoDBOperations, 
//			adapterIds, 
//			left, 
//			query, 
//			clientDedupeFilter, 
//			scanCallback, 
//			aggregation, 
//			fieldIdsAdapterPair, 
//			indexMetaData, 
//			duplicateCounts, 
//			visibilityCounts, 
//			authorizations);
		
		// TODO Auto-generated method stub
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
