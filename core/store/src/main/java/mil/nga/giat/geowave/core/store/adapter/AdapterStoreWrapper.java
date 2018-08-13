package mil.nga.giat.geowave.core.store.adapter;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;

/**
 * Given a transient store and a internal adapter store to use to map between
 * internal IDs and external IDs, we can wrap an implementation as a persistent
 * adapter store
 *
 */
public class AdapterStoreWrapper implements
		PersistentAdapterStore
{
	private final TransientAdapterStore adapterStore;
	private final InternalAdapterStore internalAdapterStore;

	public AdapterStoreWrapper(
			final TransientAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore ) {
		this.adapterStore = adapterStore;
		this.internalAdapterStore = internalAdapterStore;
	}

	@Override
	public void addAdapter(
			final InternalDataAdapter<?> adapter ) {
		adapterStore.addAdapter(adapter.getAdapter());
	}

	@Override
	public InternalDataAdapter<?> getAdapter(
			final Short internalAdapterId ) {
		if (internalAdapterId == null) {
			return null;
		}
		return new InternalDataAdapterWrapper<>(
				(WritableDataAdapter<?>) adapterStore.getAdapter(internalAdapterStore.getAdapterId(internalAdapterId)),
				internalAdapterId);
	}

	@Override
	public boolean adapterExists(
			final Short internalAdapterId ) {
		if (internalAdapterId != null) {
			return internalAdapterStore.getAdapterId(internalAdapterId) != null;

		}
		return false;
	}

	@Override
	public CloseableIterator<InternalDataAdapter<?>> getAdapters() {
		final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
		return new CloseableIteratorWrapper<>(
				it,
				Iterators.transform(
						it,
						new Function<DataAdapter<?>, InternalDataAdapter<?>>() {

							@Override
							public InternalDataAdapter<?> apply(
									final DataAdapter<?> input ) {
								return new InternalDataAdapterWrapper<>(
										(WritableDataAdapter<?>) input,
										internalAdapterStore.getInternalAdapterId(input.getAdapterId()));
							}

						}));
	}

	@Override
	public void removeAll() {
		adapterStore.removeAll();
	}

	@Override
	public void removeAdapter(
			final Short adapterId ) {
		ByteArrayId id = internalAdapterStore.getAdapterId(adapterId);
		if (id != null) {
			adapterStore.removeAdapter(id);
		}
	}

}
