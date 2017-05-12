package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.query.InputFormatHBaseRangeQuery;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRecordReader;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

/**
 * This class is used by the GeoWaveInputFormat to read data from an HBase data
 * store.
 *
 * @param <T>
 *            The native type for the reader
 */
public class GeoWaveHBaseRecordReader<T> extends
		GeoWaveRecordReader<T>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveHBaseRecordReader.class);
	protected BasicHBaseOperations operations;

	public GeoWaveHBaseRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final boolean isOutputWritable,
			final AdapterStore adapterStore,
			final BasicHBaseOperations operations ) {
		super(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore);
		this.operations = operations;
	}

	@Override
	protected CloseableIterator<?> queryRange(
			final PrimaryIndex i,
			final GeoWaveRowRange range,
			final List<QueryFilter> queryFilters,
			final QueryOptions rangeQueryOptions ) {
		return new InputFormatHBaseRangeQuery(
				adapterStore,
				i,
				HBaseSplitsProvider.unwrapRange(range),
				queryFilters,
				isOutputWritable,
				rangeQueryOptions).query(
				operations,
				adapterStore,
				rangeQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				rangeQueryOptions.getLimit());
	}

	@Override
	public boolean nextKeyValue()
			throws IOException,
			InterruptedException {
		if (iterator != null) {
			if (iterator.hasNext()) {
				++numKeysRead;
				final Object value = iterator.next();
				if (value instanceof Entry) {
					final Entry<GeoWaveInputKey, T> entry = (Entry<GeoWaveInputKey, T>) value;
					currentGeoWaveKey = entry.getKey();
					// TODO implement progress reporting
					// if (currentGeoWaveKey == null) {
					// currentAccumuloKey = null;
					// }
					// else if (currentGeoWaveKey.getInsertionId() != null) {
					// // just use the insertion ID for progress
					// currentAccumuloKey = new Key(
					// new Text(
					// currentGeoWaveKey.getInsertionId().getBytes()));
					// }
					currentValue = entry.getValue();
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public float getProgress()
			throws IOException,
			InterruptedException {
		// TODO implement progress reporting
		return 0;
	}

	// TODO implement progress reporting
	// @Override
	// public float getProgress()
	// throws IOException {
	// if ((numKeysRead > 0) && (currentAccumuloKey == null)) {
	// return 1.0f;
	// }
	// if (currentGeoWaveRangeIndexPair == null) {
	// return 0.0f;
	// }
	// final ProgressPerRange progress =
	// progressPerRange.get(currentGeoWaveRangeIndexPair);
	// if (progress == null) {
	// return getProgressForRange(
	// currentGeoWaveRangeIndexPair.getRange(),
	// currentAccumuloKey);
	// }
	// return getOverallProgress(
	// currentGeoWaveRangeIndexPair.getRange(),
	// currentAccumuloKey,
	// progress);
	// }
	//
	// private static float getOverallProgress(
	// final Range range,
	// final Key currentKey,
	// final ProgressPerRange progress ) {
	// final float rangeProgress = getProgressForRange(
	// range,
	// currentKey);
	// return progress.getOverallProgress(rangeProgress);
	// }
	//
	// private static float getProgressForRange(
	// final ByteSequence start,
	// final ByteSequence end,
	// final ByteSequence position ) {
	// final int maxDepth = Math.min(
	// Math.max(
	// end.length(),
	// start.length()),
	// position.length());
	// final BigInteger startBI = new BigInteger(
	// AccumuloMRUtils.extractBytes(
	// start,
	// maxDepth));
	// final BigInteger endBI = new BigInteger(
	// AccumuloMRUtils.extractBytes(
	// end,
	// maxDepth));
	// final BigInteger positionBI = new BigInteger(
	// AccumuloMRUtils.extractBytes(
	// position,
	// maxDepth));
	// return (float) (positionBI.subtract(
	// startBI).doubleValue() / endBI.subtract(
	// startBI).doubleValue());
	// }
	//
	// private static float getProgressForRange(
	// final Range range,
	// final Key currentKey ) {
	// if (currentKey == null) {
	// return 0f;
	// }
	// if ((range.getStartKey() != null) && (range.getEndKey() != null)) {
	// if (!range.getStartKey().equals(
	// range.getEndKey(),
	// PartialKey.ROW)) {
	// // just look at the row progress
	// return getProgressForRange(
	// range.getStartKey().getRowData(),
	// range.getEndKey().getRowData(),
	// currentKey.getRowData());
	// }
	// else if (!range.getStartKey().equals(
	// range.getEndKey(),
	// PartialKey.ROW_COLFAM)) {
	// // just look at the column family progress
	// return getProgressForRange(
	// range.getStartKey().getColumnFamilyData(),
	// range.getEndKey().getColumnFamilyData(),
	// currentKey.getColumnFamilyData());
	// }
	// else if (!range.getStartKey().equals(
	// range.getEndKey(),
	// PartialKey.ROW_COLFAM_COLQUAL)) {
	// // just look at the column qualifier progress
	// return getProgressForRange(
	// range.getStartKey().getColumnQualifierData(),
	// range.getEndKey().getColumnQualifierData(),
	// currentKey.getColumnQualifierData());
	// }
	// }
	// // if we can't figure it out, then claim no progress
	// return 0f;
	// }
}