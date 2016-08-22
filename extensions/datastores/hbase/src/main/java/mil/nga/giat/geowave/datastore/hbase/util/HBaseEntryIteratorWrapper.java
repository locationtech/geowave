package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.EntryIteratorWrapper;

public class HBaseEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(HBaseEntryIteratorWrapper.class);

	public HBaseEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter ) {
		super(
				true,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				null);
	}

	public HBaseEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		super(
				true,
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
		Result result = null;
		try {
			result = (Result) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not an HBase row Result.");
			return null;
		}
		return HBaseUtils.decodeRow(
				result,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

}
