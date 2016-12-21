package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.NativeGeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class NativeEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(NativeEntryIteratorWrapper.class);

	public NativeEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter ) {
		super(
				false,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				null);
	}

	public NativeEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		super(
				false,
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
		NativeGeoWaveRow entry = null;
		try {
			entry = (NativeGeoWaveRow) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not a native geowave row entry.");
			return null;
		}
		return DataStoreUtils.decodeRow(
				entry,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}
}
