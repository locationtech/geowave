package mil.nga.giat.geowave.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;

public class JobContextInternalAdapterStore implements
		InternalAdapterStore
{
	private static final Class<?> CLASS = JobContextInternalAdapterStore.class;
	private final JobContext context;
	private final InternalAdapterStore persistentInternalAdapterStore;
	protected final BiMap<ByteArrayId, Short> cache = HashBiMap.create();

	public JobContextInternalAdapterStore(
			final JobContext context,
			final InternalAdapterStore persistentInternalAdapterStore ) {
		this.context = context;
		this.persistentInternalAdapterStore = persistentInternalAdapterStore;
	}

	@Override
	public ByteArrayId getAdapterId(
			final short internalAdapterId ) {
		ByteArrayId adapterId = cache.inverse().get(
				internalAdapterId);
		if (adapterId == null) {
			adapterId = getAdapterIdInternal(internalAdapterId);
		}
		return adapterId;
	}

	private ByteArrayId getAdapterIdInternal(
			final short internalAdapterId ) {
		// first try to get it from the job context
		ByteArrayId adapterId = getAdapterIdFromJobContext(internalAdapterId);
		if (adapterId == null) {
			// then try to get it from the persistent store
			adapterId = persistentInternalAdapterStore.getAdapterId(internalAdapterId);
		}

		if (adapterId != null) {
			cache.put(
					adapterId,
					internalAdapterId);
		}
		return adapterId;
	}

	private Short getInternalAdapterIdInternal(
			final ByteArrayId adapterId ) {
		// first try to get it from the job context
		Short internalAdapterId = getInternalAdapterIdFromJobContext(adapterId);
		if (internalAdapterId == null) {
			// then try to get it from the persistent store
			internalAdapterId = persistentInternalAdapterStore.getInternalAdapterId(adapterId);
		}

		if (internalAdapterId != null) {
			cache.put(
					adapterId,
					internalAdapterId);
		}
		return internalAdapterId;
	}

	@Override
	public Short getInternalAdapterId(
			final ByteArrayId adapterId ) {
		Short internalAdapterId = cache.get(adapterId);
		if (internalAdapterId == null) {
			internalAdapterId = getInternalAdapterIdInternal(adapterId);
		}
		return internalAdapterId;
	}

	protected Short getInternalAdapterIdFromJobContext(
			final ByteArrayId adapterId ) {
		return GeoWaveConfiguratorBase.getInternalAdapterId(
				CLASS,
				context,
				adapterId);
	}

	protected ByteArrayId getAdapterIdFromJobContext(
			final short internalAdapterId ) {
		return GeoWaveConfiguratorBase.getAdapterId(
				CLASS,
				context,
				internalAdapterId);
	}

	@Override
	public short addAdapterId(
			final ByteArrayId adapterId ) {
		return persistentInternalAdapterStore.addAdapterId(adapterId);
	}

	@Override
	public boolean remove(
			final ByteArrayId adapterId ) {
		return persistentInternalAdapterStore.remove(adapterId);
	}

	public static void addInternalDataAdapter(
			final Configuration configuration,
			final ByteArrayId adapterId,
			final short internalAdapterId ) {
		GeoWaveConfiguratorBase.addInternalAdapterId(
				CLASS,
				configuration,
				adapterId,
				internalAdapterId);
	}

	@Override
	public boolean remove(
			final short internalAdapterId ) {
		cache.inverse().remove(
				internalAdapterId);
		return persistentInternalAdapterStore.remove(internalAdapterId);
	}

	@Override
	public void removeAll() {
		cache.clear();
		persistentInternalAdapterStore.removeAll();
	}

}
