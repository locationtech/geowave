package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * This class implements an index store by first checking the job context for an
 * index and keeping a local cache of indices that have been discovered. It will
 * check the metadata store if it cannot find an index in the job context.
 */
public class JobContextIndexStore implements
		IndexStore
{
	private static final Class<?> CLASS = JobContextIndexStore.class;
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
		Index index = getIndex(
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
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				accumuloOperations);
		final CloseableIterator<Index> it = indexStore.getIndices();
		// cache any results
		return new CloseableIteratorWrapper<Index>(
				it,
				IteratorUtils.transformedIterator(
						it,
						new Transformer() {

							@Override
							public Object transform(
									final Object obj ) {
								if (obj instanceof Index) {
									indexCache.put(
											((Index) obj).getId(),
											(Index) obj);
								}
								return obj;
							}
						}));
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		GeoWaveConfiguratorBase.addIndex(
				CLASS,
				config,
				index);
	}

	protected static Index getIndex(
			final JobContext context,
			final ByteArrayId indexId ) {
		return GeoWaveConfiguratorBase.getIndex(
				CLASS,
				context,
				indexId);
	}

	public static Index[] getIndices(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getIndices(
				CLASS,
				context);
	}

}
