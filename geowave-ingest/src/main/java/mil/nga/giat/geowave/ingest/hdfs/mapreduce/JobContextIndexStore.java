package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * This class implements an index store by first checking the job context for an
 * index and keeping a local cache of indices that have been discovered. It will
 * check the metadata store if it cannot find an index in the job context.
 */
public class JobContextIndexStore implements
		IndexStore
{

	private final JobContext context;
	private final AccumuloOperations accumuloOperations;
	private final Map<ByteArrayId, Index> indexCache = new HashMap<ByteArrayId, Index>();

	public JobContextIndexStore(
			final JobContext context,
			final AccumuloOperations accumuloOperations ) {
		this.context = context;
		this.accumuloOperations = accumuloOperations;

	}

	@Override
	public void addIndex(
			final Index index ) {
		indexCache.put(
				index.getId(),
				index);
	}

	@Override
	public Index getIndex(
			final ByteArrayId indexId ) {
		Index index = indexCache.get(indexId);
		if (index == null) {
			index = getIndexInternal(indexId);
		}
		return index;
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId ) {
		if (indexCache.containsKey(indexId)) {
			return true;
		}
		final Index index = getIndexInternal(indexId);
		return index != null;
	}

	private Index getIndexInternal(
			final ByteArrayId indexId ) {
		// first try to get it from the job context
		Index index = GeoWaveOutputFormat.getIndex(
				context,
				indexId);
		if (index == null) {
			// then try to get it from the accumulo persistent store
			final AccumuloIndexStore indexStore = new AccumuloIndexStore(
					accumuloOperations);
			index = indexStore.getIndex(indexId);
		}

		if (index != null) {
			indexCache.put(
					indexId,
					index);
		}
		return index;
	}

	@Override
	public CloseableIterator<Index> getIndices() {
		// this should not be called but just return what is in the accumulo
		// adapter store
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				accumuloOperations);
		return indexStore.getIndices();
	}

}
