package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class NativeEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(NativeEntryIteratorWrapper.class);
	private final boolean decodePersistenceEncoding;

	public NativeEntryIteratorWrapper(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
			final boolean decodePersistenceEncoding ) {
		super(
				dataStore,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
		this.decodePersistenceEncoding = decodePersistenceEncoding;
	}

	@Override
	protected T decodeRow(
			final GeoWaveRow row,
			final QueryFilter clientFilter,
			final PrimaryIndex index ) {
		return (T) BaseDataStoreUtils.decodeRow(
				row,
				clientFilter,
				null,
				adapterStore,
				index,
				scanCallback,
				null,
				decodePersistenceEncoding);
	}
}
