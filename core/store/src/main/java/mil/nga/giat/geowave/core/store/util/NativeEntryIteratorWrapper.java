package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class NativeEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(NativeEntryIteratorWrapper.class);

	public NativeEntryIteratorWrapper(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
			final boolean decodePersistenceEncoding ) {
		super(
				false,
				dataStore,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
	}

	@Override
	protected T decodeRow(
			final Object row,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final boolean wholeRowEncoding ) {
		GeoWaveRow entry = null;
		try {
			entry = (GeoWaveRow) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not a native geowave row entry.");
			return null;
		}
		return (T) dataStore.decodeRow(
				entry,
				wholeRowEncoding,
				clientFilter,
				null,
				adapterStore,
				index,
				scanCallback,
				null,
				false);
	}
}
