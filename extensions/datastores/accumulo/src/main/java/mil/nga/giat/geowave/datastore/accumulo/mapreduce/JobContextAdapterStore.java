package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * This class implements an adapter store by first checking the job context for
 * an adapter and keeping a local cache of adapters that have been discovered.
 * It will check the metadata store if it cannot find an adapter in the job
 * context.
 */
public class JobContextAdapterStore implements
		AdapterStore
{
	private static final Class<?> CLASS = JobContextAdapterStore.class;
	private final JobContext context;
	private final AccumuloOperations accumuloOperations;
	private final Map<ByteArrayId, DataAdapter<?>> adapterCache = new HashMap<ByteArrayId, DataAdapter<?>>();

	public JobContextAdapterStore(
			final JobContext context,
			final AccumuloOperations accumuloOperations ) {
		this.context = context;
		this.accumuloOperations = accumuloOperations;

	}

	@Override
	public void addAdapter(
			final DataAdapter<?> adapter ) {
		adapterCache.put(
				adapter.getAdapterId(),
				adapter);
	}

	@Override
	public DataAdapter<?> getAdapter(
			final ByteArrayId adapterId ) {
		DataAdapter<?> adapter = adapterCache.get(adapterId);
		if (adapter == null) {
			adapter = getAdapterInternal(adapterId);
		}
		return adapter;
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

	private DataAdapter<?> getAdapterInternal(
			final ByteArrayId adapterId ) {
		// first try to get it from the job context
		DataAdapter<?> adapter = getDataAdapter(
				context,
				adapterId);
		if (adapter == null) {
			// then try to get it from the accumulo persistent store
			final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
					accumuloOperations);
			adapter = adapterStore.getAdapter(adapterId);
		}

		if (adapter != null) {
			adapterCache.put(
					adapterId,
					adapter);
		}
		return adapter;
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);
		final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
		// cache any results
		return new CloseableIteratorWrapper<DataAdapter<?>>(
				it,
				IteratorUtils.transformedIterator(
						it,
						new Transformer() {

							@Override
							public Object transform(
									final Object obj ) {
								if (obj instanceof DataAdapter) {
									adapterCache.put(
											((DataAdapter) obj).getAdapterId(),
											(DataAdapter) obj);
								}
								return obj;
							}
						}));
	}

	public List<ByteArrayId> getAdapterIds() {
		final DataAdapter<?>[] userAdapters = GeoWaveConfiguratorBase.getDataAdapters(
				CLASS,
				context);
		if ((userAdapters == null) || (userAdapters.length <= 0)) {
			return IteratorUtils.toList(IteratorUtils.transformedIterator(
					getAdapters(),
					new Transformer() {

						@Override
						public Object transform(
								final Object input ) {
							if (input instanceof DataAdapter) {
								return ((DataAdapter) input).getAdapterId();
							}
							return input;
						}
					}));
		}
		else {
			final List<ByteArrayId> retVal = new ArrayList<ByteArrayId>(
					userAdapters.length);
			for (final DataAdapter<?> adapter : userAdapters) {
				retVal.add(adapter.getAdapterId());
			}
			return retVal;
		}
	}

	protected static DataAdapter<?> getDataAdapter(
			final JobContext context,
			final ByteArrayId adapterId ) {
		return GeoWaveConfiguratorBase.getDataAdapter(
				CLASS,
				context,
				adapterId);
	}

	public static DataAdapter<?>[] getDataAdapters(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getDataAdapters(
				CLASS,
				context);
	}

	public static void addDataAdapter(
			final Configuration configuration,
			final DataAdapter<?> adapter ) {
		GeoWaveConfiguratorBase.addDataAdapter(
				CLASS,
				configuration,
				adapter);
	}

}
