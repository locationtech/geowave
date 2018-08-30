package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;

public class CassandraStatisticsIterator implements
		CloseableIterator<GeoWaveMetadata>
{
	final private CloseableIterator<GeoWaveMetadata> it;
	private DataStatistics<?> nextVal = null;

	public CassandraStatisticsIterator(
			final CloseableIterator<GeoWaveMetadata> resultIterator ) {
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
			final GeoWaveMetadata row = it.next();

			final DataStatistics<?> statEntry = entryToValue(row);

			if (currentStatistics == null) {
				currentStatistics = statEntry;
			}
			else {
				if (statEntry.getStatisticsId().equals(
						currentStatistics.getStatisticsId()) && statEntry.getInternalDataAdapterId().equals(
						currentStatistics.getInternalDataAdapterId())) {
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

	protected DataStatistics<?> entryToValue(
			final GeoWaveMetadata entry ) {
		final DataStatistics<?> stats = (DataStatistics<?>) PersistenceUtils.fromBinary(entry.getValue());

		if (stats != null) {
			stats.setInternalDataAdapterId(ByteArrayUtils.byteArrayToShort(entry.getSecondaryId()));
			stats.setStatisticsId(new ByteArrayId(
					entry.getPrimaryId()));
		}

		return stats;
	}

	protected GeoWaveMetadata statsToMetadata(
			final DataStatistics<?> stats ) {
		return new GeoWaveMetadata(
				stats.getStatisticsId().getBytes(),
				ByteArrayUtils.shortToByteArray(stats.getInternalDataAdapterId()),
				null,
				PersistenceUtils.toBinary(stats));
	}

	@Override
	public void close()
			throws IOException {
		it.close();
	}
}