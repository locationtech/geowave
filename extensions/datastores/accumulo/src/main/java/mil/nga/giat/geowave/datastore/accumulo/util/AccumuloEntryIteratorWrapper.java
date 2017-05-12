package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.EntryIteratorWrapper;

/**
 * This is used internally to translate Accumulo rows into native objects (using
 * the appropriate data adapter). It also performs any client-side filtering. It
 * will peek at the next entry in the accumulo iterator to always maintain a
 * reference to the next value.
 *
 * @param <T>
 *            The type for the entry
 */
public class AccumuloEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloEntryIteratorWrapper.class);

	public AccumuloEntryIteratorWrapper(
			final boolean wholeRowEncoding,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter ) {
		super(
				wholeRowEncoding,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				null);
	}

	public AccumuloEntryIteratorWrapper(
			final boolean wholeRowEncoding,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		super(
				wholeRowEncoding,
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
		Entry<Key, Value> entry = null;
		try {
			entry = (Entry<Key, Value>) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error(
					"Row is not an accumulo row entry.",
					e);
			return null;
		}
		return AccumuloUtils.decodeRow(
				entry.getKey(),
				entry.getValue(),
				wholeRowEncoding,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

}
