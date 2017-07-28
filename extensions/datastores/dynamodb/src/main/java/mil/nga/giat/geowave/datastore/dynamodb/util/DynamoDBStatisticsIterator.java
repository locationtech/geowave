package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;

public class DynamoDBStatisticsIterator implements
		CloseableIterator<GeoWaveMetadata>
{
	final private Iterator<Map<String, AttributeValue>> it;
	private DataStatistics<?> nextVal = null;

	public DynamoDBStatisticsIterator(
			final Iterator<Map<String, AttributeValue>> resultIterator ) {
		it = resultIterator;
	}

	@Override
	public boolean hasNext() {
		return (nextVal != null) || it.hasNext();
	}

	@Override
	public GeoWaveMetadata next() {
		DataStatistics<?> currentStatistics = nextVal;

		nextVal = null;
		while (it.hasNext()) {
			final Map<String, AttributeValue> row = it.next();

			final DataStatistics<?> statEntry = entryToValue(row);

			if (currentStatistics == null) {
				currentStatistics = statEntry;
			}
			else {
				if (statEntry.getStatisticsId().equals(
						currentStatistics.getStatisticsId()) && statEntry.getDataAdapterId().equals(
						currentStatistics.getDataAdapterId())) {
					currentStatistics.merge(statEntry);
				}
				else {
					nextVal = statEntry;
					break;
				}
			}
		}

		return statsToMetadata(currentStatistics);
	}

	@Override
	public void close() {
		// Close is a no-op for dynamodb client
	}

	protected DataStatistics<?> entryToValue(
			final Map<String, AttributeValue> entry ) {
		final DataStatistics<?> stats = (DataStatistics<?>) PersistenceUtils.fromBinary(DynamoDBUtils.getValue(entry));

		if (stats != null) {
			stats.setDataAdapterId(new ByteArrayId(
					DynamoDBUtils.getSecondaryId(entry)));
		}

		return stats;
	}

	protected GeoWaveMetadata statsToMetadata(
			DataStatistics<?> stats ) {
		return new GeoWaveMetadata(
				stats.getStatisticsId().getBytes(),
				stats.getDataAdapterId().getBytes(),
				null,
				PersistenceUtils.toBinary(stats));
	}
}
