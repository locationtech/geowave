/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> JobContextAdapterStore </code>
 */
public class JobContextHBaseAdapterStore implements
		AdapterStore
{

	private static final Class<?> CLASS = JobContextHBaseAdapterStore.class;
	private final JobContext context;
	private final BasicHBaseOperations operations;
	private final Map<ByteArrayId, DataAdapter<?>> adapterCache = new HashMap<ByteArrayId, DataAdapter<?>>();
	protected static final Logger LOGGER = Logger.getLogger(CLASS);

	public JobContextHBaseAdapterStore(
			final JobContext context,
			final BasicHBaseOperations operations ) {
		this.context = context;
		this.operations = operations;

	}

	@Override
	public void addAdapter(
			DataAdapter<?> adapter ) {
		adapterCache.put(
				adapter.getAdapterId(),
				adapter);
	}

	@Override
	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId ) {
		DataAdapter<?> adapter = adapterCache.get(adapterId);
		if (adapter == null) {
			adapter = getAdapterInternal(adapterId);
		}
		return adapter;
	}

	private DataAdapter<?> getAdapterInternal(
			final ByteArrayId adapterId ) {
		// first try to get it from the job context
		DataAdapter<?> adapter = getDataAdapter(
				context,
				adapterId);
		if (adapter == null) {
			// then try to get it from the accumulo persistent store
			final HBaseAdapterStore adapterStore = new HBaseAdapterStore(
					operations);
			adapter = adapterStore.getAdapter(adapterId);
		}

		if (adapter != null) {
			adapterCache.put(
					adapterId,
					adapter);
		}
		return adapter;
	}

	protected static DataAdapter<?> getDataAdapter(
			final JobContext context,
			final ByteArrayId adapterId ) {
		return GeoWaveHBaseConfiguratorBase.getDataAdapter(
				CLASS,
				context,
				adapterId);
	}

	@Override
	public boolean adapterExists(
			final ByteArrayId adapterId ) {
		if (adapterCache.containsKey(adapterId)) {
			return true;
		}
		final DataAdapter<?> adapter = getAdapterInternal(adapterId);
		return adapter != null;
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		// TODO #406 Need to fix
		LOGGER.warn("Need to code this method getAdapters");
		return null;
	}

	public static DataAdapter<?>[] getDataAdapters(
			final JobContext context ) {
		return GeoWaveHBaseConfiguratorBase.getDataAdapters(
				CLASS,
				context);
	}

	public static void addDataAdapter(
			final Configuration configuration,
			final DataAdapter<?> adapter ) {
		GeoWaveHBaseConfiguratorBase.addDataAdapter(
				CLASS,
				configuration,
				adapter);
	}
}
