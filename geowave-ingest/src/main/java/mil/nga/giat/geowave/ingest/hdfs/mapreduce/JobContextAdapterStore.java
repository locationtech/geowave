package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.hadoop.mapreduce.JobContext;

public class JobContextAdapterStore implements AdapterStore {
	private final JobContext context;
	private final AccumuloOperations accumuloOperations;
	private final Map<ByteArrayId, DataAdapter<?>> adapterCache = new HashMap<ByteArrayId, DataAdapter<?>>();

	public JobContextAdapterStore( final JobContext context, final AccumuloOperations accumuloOperations ) {
		this.context = context;
		this.accumuloOperations = accumuloOperations;

	}

	@Override
	public void addAdapter( final DataAdapter<?> adapter ) {
		adapterCache.put(adapter.getAdapterId(), adapter);
	}

	@Override
	public DataAdapter<?> getAdapter( final ByteArrayId adapterId ) {
		DataAdapter<?> adapter = adapterCache.get(adapterId);
		if (adapter == null) {
			adapter = getAdapterInternal(adapterId);
		}
		return adapter;
	}

	@Override
	public boolean adapterExists( final ByteArrayId adapterId ) {
		if (adapterCache.containsKey(adapterId)) { return true; }
		final DataAdapter<?> adapter = getAdapterInternal(adapterId);
		return adapter != null;
	}

	private DataAdapter<?> getAdapterInternal( final ByteArrayId adapterId ) {
		// first try to get it from the job context
		DataAdapter<?> adapter = GeoWaveOutputFormat.getDataAdapter(context, adapterId);
		if (adapter == null) {
			// then try to get it from the accumulo persistent store
			final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(accumuloOperations);
			adapter = adapterStore.getAdapter(adapterId);
		}

		if (adapter != null) {
			adapterCache.put(adapterId, adapter);
		}
		return adapter;
	}

	@Override
	public Iterator<DataAdapter<?>> getAdapters() {
		// this should not be called but just return what is in the accumulo
		// adapter store
		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(accumuloOperations);
		return adapterStore.getAdapters();
	}

}
