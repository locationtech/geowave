package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.query.InputFormatAccumuloRangeQuery;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRecordReader;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

/**
 * This class is used by the GeoWaveInputFormat to read data from an Accumulo
 * data store.
 *
 * @param <T>
 *            The native type for the reader
 */
public class GeoWaveAccumuloRecordReader<T> extends
		GeoWaveRecordReader<T>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveAccumuloRecordReader.class);
	protected Key currentAccumuloKey = null;
	protected AccumuloOperations accumuloOperations;

	public GeoWaveAccumuloRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final boolean isOutputWritable,
			final AdapterStore adapterStore,
			final AccumuloOperations accumuloOperations ) {
		super(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore);
		this.accumuloOperations = accumuloOperations;
	}

	@Override
	protected CloseableIterator<?> queryRange(
			PrimaryIndex i,
			GeoWaveRowRange range,
			List<QueryFilter> queryFilters,
			QueryOptions rangeQueryOptions ) {
		return new InputFormatAccumuloRangeQuery(
				adapterStore,
				i,
				AccumuloSplitsProvider.unwrapRange(range),
				queryFilters,
				isOutputWritable,
				rangeQueryOptions).query(
				accumuloOperations,
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
					if (currentGeoWaveKey == null) {
						currentAccumuloKey = null;
					}
					else if (currentGeoWaveKey.getInsertionId() != null) {
						// just use the insertion ID for progress
						currentAccumuloKey = new Key(
								new Text(
										currentGeoWaveKey.getInsertionId().getBytes()));
					}
					currentValue = entry.getValue();
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public float getProgress()
			throws IOException {
		if ((numKeysRead > 0) && (currentAccumuloKey == null)) {
			return 1.0f;
		}
		if (currentGeoWaveRangeIndexPair == null) {
			return 0.0f;
		}
		final ProgressPerRange progress = progressPerRange.get(currentGeoWaveRangeIndexPair);
		if (progress == null) {
			return getProgressForRange(
					AccumuloSplitsProvider.unwrapRange(currentGeoWaveRangeIndexPair.getRange()),
					currentAccumuloKey);
		}
		return getOverallProgress(
				AccumuloSplitsProvider.unwrapRange(currentGeoWaveRangeIndexPair.getRange()),
				currentAccumuloKey,
				progress);
	}

	private static float getOverallProgress(
			final Range range,
			final Key currentKey,
			final ProgressPerRange progress ) {
		final float rangeProgress = getProgressForRange(
				range,
				currentKey);
		return progress.getOverallProgress(rangeProgress);
	}

	private static float getProgressForRange(
			final ByteSequence start,
			final ByteSequence end,
			final ByteSequence position ) {
		final int maxDepth = Math.min(
				Math.max(
						end.length(),
						start.length()),
				position.length());
		final BigInteger startBI = new BigInteger(
				SplitsProvider.extractBytes(
						start.getBackingArray(),
						maxDepth));
		final BigInteger endBI = new BigInteger(
				SplitsProvider.extractBytes(
						end.getBackingArray(),
						maxDepth));
		final BigInteger positionBI = new BigInteger(
				SplitsProvider.extractBytes(
						position.getBackingArray(),
						maxDepth));
		return (float) (positionBI.subtract(
				startBI).doubleValue() / endBI.subtract(
				startBI).doubleValue());
	}

	private static float getProgressForRange(
			final Range range,
			final Key currentKey ) {
		if (currentKey == null) {
			return 0f;
		}
		if ((range.getStartKey() != null) && (range.getEndKey() != null)) {
			if (!range.getStartKey().equals(
					range.getEndKey(),
					PartialKey.ROW)) {
				// just look at the row progress
				return getProgressForRange(
						range.getStartKey().getRowData(),
						range.getEndKey().getRowData(),
						currentKey.getRowData());
			}
			else if (!range.getStartKey().equals(
					range.getEndKey(),
					PartialKey.ROW_COLFAM)) {
				// just look at the column family progress
				return getProgressForRange(
						range.getStartKey().getColumnFamilyData(),
						range.getEndKey().getColumnFamilyData(),
						currentKey.getColumnFamilyData());
			}
			else if (!range.getStartKey().equals(
					range.getEndKey(),
					PartialKey.ROW_COLFAM_COLQUAL)) {
				// just look at the column qualifier progress
				return getProgressForRange(
						range.getStartKey().getColumnQualifierData(),
						range.getEndKey().getColumnQualifierData(),
						currentKey.getColumnQualifierData());
			}
		}
		// if we can't figure it out, then claim no progress
		return 0f;
	}
}