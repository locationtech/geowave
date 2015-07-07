package mil.nga.giat.geowave.core.ingest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestUtils.class);

	public static <T> void ingest(
			final T input,
			final IngestCommandLineOptions ingestOptions,
			final IngestPluginBase<T, ?> ingestPlugin,
			final IndexProvider indexProvider,
			final IngestRunData ingestRunData )
			throws IOException {
		final Index primaryIndex = ingestOptions.getIndex(indexProvider.getSupportedIndices());
		if (primaryIndex == null) {
			LOGGER.error("Could not get index instance, getIndex() returned null;");
			throw new IOException(
					"Could not get index instance, getIndex() returned null");
		}
		final IndexWriter primaryIndexWriter = ingestRunData.getIndexWriter(primaryIndex);
		final Index idx = primaryIndexWriter.getIndex();
		if (idx == null) {
			LOGGER.error("Could not get index instance, getIndex() returned null;");
			throw new IOException(
					"Could not get index instance, getIndex() returned null");
		}

		final Map<ByteArrayId, Index> requiredIndexMap = new HashMap<ByteArrayId, Index>();
		final Index[] requiredIndices = indexProvider.getRequiredIndices();
		if ((requiredIndices != null) && (requiredIndices.length > 0)) {
			for (final Index requiredIndex : requiredIndices) {
				requiredIndexMap.put(
						requiredIndex.getId(),
						requiredIndex);
			}
		}
		try (CloseableIterator<?> geowaveDataIt = ingestPlugin.toGeoWaveData(
				input,
				idx.getId(),
				ingestOptions.getVisibility())) {
			while (geowaveDataIt.hasNext()) {
				final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
				final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
				if (adapter == null) {
					LOGGER.warn("Adapter not found for " + geowaveData.getValue());
					continue;
				}
				final IndexWriter indexWriter;
				if (idx.getId().equals(
						geowaveData.getIndexId())) {
					indexWriter = primaryIndexWriter;
				}
				else {
					final Index index = requiredIndexMap.get(geowaveData.getIndexId());
					if (index == null) {
						LOGGER.warn("Index '" + geowaveData.getIndexId().getString() + "' not found for " + geowaveData.getValue());
						continue;
					}
					indexWriter = ingestRunData.getIndexWriter(index);
				}
				indexWriter.write(
						adapter,
						geowaveData.getValue());
			}
		}
	}
}
