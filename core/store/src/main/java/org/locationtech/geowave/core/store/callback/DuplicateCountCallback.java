package org.locationtech.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.util.DataStoreUtils;

/**
 * This callback finds the duplicates for each scanned entry, and deletes them
 * by insertion ID
 */
public class DuplicateCountCallback<T> implements
		ScanCallback<T, GeoWaveRow>,
		Closeable
{
	private long numDuplicates;

	public DuplicateCountCallback() {

		numDuplicates = 0;
	}

	public long getDuplicateCount() {
		return numDuplicates;
	}

	@Override
	public void close()
			throws IOException {

	}

	@Override
	public void entryScanned(
			final T entry,
			GeoWaveRow row ) {

		final int rowDups = row.getNumberOfDuplicates();
		numDuplicates += rowDups;

	}
}
