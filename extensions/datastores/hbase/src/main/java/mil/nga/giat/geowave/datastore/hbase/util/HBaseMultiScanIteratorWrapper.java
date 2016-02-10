package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class HBaseMultiScanIteratorWrapper<T> extends
		HBaseEntryIteratorWrapper<T>
{

	private final static Logger LOGGER = Logger.getLogger(
			HBaseMultiScanIteratorWrapper.class);
	private final Iterator<Iterator<Result>> scanIterators;
	private Iterator<Result> currentScanIt;

	public HBaseMultiScanIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Iterator<Result>> scanIterators,
			final QueryFilter clientFilter ) {
		super(
				adapterStore,
				index,
				null,
				clientFilter);
		this.scanIterators = scanIterators;
	}

	public HBaseMultiScanIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Iterator<Result>> scanIterators,
			final QueryFilter clientFilter,
			final ScanCallback scanCallback ) {
		super(
				adapterStore,
				index,
				null,
				clientFilter,
				scanCallback);
		this.scanIterators = scanIterators;
	}

	@Override
	protected void findNext() {
		while ((currentScanIt == null) && scanIterators.hasNext()) {
			currentScanIt = scanIterators.next();
		}

		if (currentScanIt == null) {
			// ran out of scans
			return;
		}

		while ((nextValue == null) && (currentScanIt.hasNext() || scanIterators.hasNext())) {

			while (!currentScanIt.hasNext() && scanIterators.hasNext()) {
				currentScanIt = scanIterators.next();
			}
			if (currentScanIt.hasNext()) {
				final Result row = currentScanIt.next();

				final T decodedValue = decodeRow(
						row,
						clientFilter,
						index);

				if (decodedValue != null) {
					nextValue = decodedValue;
					return;
				}
			}
		}
	}

}
