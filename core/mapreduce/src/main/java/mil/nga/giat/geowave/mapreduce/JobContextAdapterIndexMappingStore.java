package mil.nga.giat.geowave.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;

/**
 * This class implements an adapter index mapping store by first checking the
 * job context for an adapter and keeping a local cache of adapters that have
 * been discovered. It will check the metadata store if it cannot find an
 * adapter in the job context.
 */
public class JobContextAdapterIndexMappingStore implements
		AdapterIndexMappingStore
{
	private static final Class<?> CLASS = JobContextAdapterIndexMappingStore.class;
	private final JobContext context;
	private final AdapterIndexMappingStore persistentAdapterIndexMappingStore;
	private final Map<ByteArrayId, AdapterToIndexMapping> adapterCache = new HashMap<ByteArrayId, AdapterToIndexMapping>();

	public JobContextAdapterIndexMappingStore(
			final JobContext context,
			final AdapterIndexMappingStore persistentAdapterIndexMappingStore ) {
		this.context = context;
		this.persistentAdapterIndexMappingStore = persistentAdapterIndexMappingStore;

	}

	private AdapterToIndexMapping getIndicesForAdapterInternal(
			final ByteArrayId adapterId ) {
		// first try to get it from the job context
		AdapterToIndexMapping adapter = getAdapterToIndexMapping(
				context,
				adapterId);
		if (adapter == null) {
			// then try to get it from the persistent store
			adapter = persistentAdapterIndexMappingStore.getIndicesForAdapter(adapterId);
		}

		if (adapter != null) {
			adapterCache.put(
					adapterId,
					adapter);
		}
		return adapter;
	}

	@Override
	public void removeAll() {
		adapterCache.clear();
	}

	protected static AdapterToIndexMapping getAdapterToIndexMapping(
			final JobContext context,
			final ByteArrayId adapterId ) {
		return GeoWaveConfiguratorBase.getAdapterToIndexMapping(
				CLASS,
				context,
				adapterId);
	}

	public static void addAdapterToIndexMapping(
			final Configuration configuration,
			final AdapterToIndexMapping adapter ) {
		GeoWaveConfiguratorBase.addAdapterToIndexMapping(
				CLASS,
				configuration,
				adapter);
	}

	@Override
	public AdapterToIndexMapping getIndicesForAdapter(
			ByteArrayId adapterId ) {
		AdapterToIndexMapping adapter = adapterCache.get(adapterId);
		if (adapter == null) {
			adapter = getIndicesForAdapterInternal(adapterId);
		}
		return adapter;
	}

	@Override
	public void addAdapterIndexMapping(
			AdapterToIndexMapping mapping )
			throws MismatchedIndexToAdapterMapping {
		adapterCache.put(
				mapping.getAdapterId(),
				mapping);
	}

	@Override
	public void remove(
			ByteArrayId adapterId ) {
		adapterCache.remove(adapterId);
	}

}
