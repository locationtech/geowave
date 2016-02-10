/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy Functionality similar to <code> JobContextIndexStore </code>
 */
public class JobContextHBaseIndexStore implements
		IndexStore
{

	private static final Class<?> CLASS = JobContextHBaseIndexStore.class;
	private final JobContext context;
	private final BasicHBaseOperations operations;
	private final Map<ByteArrayId, Index> indexCache = new HashMap<ByteArrayId, Index>();
	protected static final Logger LOGGER = Logger.getLogger(
			CLASS);

	public JobContextHBaseIndexStore(
			final JobContext context,
			final BasicHBaseOperations operations ) {
		this.context = context;
		this.operations = operations;

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
		Index index = indexCache.get(
				indexId);
		if (index == null) {
			index = getIndexInternal(
					indexId);
		}
		return index;
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId ) {
		if (indexCache.containsKey(
				indexId)) {
			return true;
		}
		final Index index = getIndexInternal(
				indexId);
		return index != null;
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices() {
		// TODO #406 Need to fix
		LOGGER.warn(
				"Need to code this method getIndices");
		return null;
	}

	public static Index[] getIndices(
			final JobContext context ) {
		return GeoWaveHBaseConfiguratorBase.getIndices(
				CLASS,
				context);
	}

	private Index getIndexInternal(
			final ByteArrayId indexId ) {
		// first try to get it from the job context
		Index index = getIndex(
				context,
				indexId);
		if (index == null) {
			// then try to get it from the accumulo persistent store
			final HBaseIndexStore indexStore = new HBaseIndexStore(
					operations);
			index = indexStore.getIndex(
					indexId);
		}

		if (index != null) {
			indexCache.put(
					indexId,
					index);
		}
		return index;
	}

	protected static Index getIndex(
			final JobContext context,
			final ByteArrayId indexId ) {
		return GeoWaveHBaseConfiguratorBase.getIndex(
				CLASS,
				context,
				indexId);
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		GeoWaveHBaseConfiguratorBase.addIndex(
				CLASS,
				config,
				index);
	}
}
