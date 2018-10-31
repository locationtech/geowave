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
public class DuplicateDeletionCallback<T> implements
		DeleteCallback<T, GeoWaveRow>,
		Closeable
{
	private BaseDataStore dataStore;
	private InternalDataAdapter adapter;
	private Index index;
	private HashMap<ByteArray, InsertionIdData> duplicateInsertionIds;
	private List<InsertionIdData> origRows;

	private boolean closed = false;

	public DuplicateDeletionCallback(
			final BaseDataStore store,
			final InternalDataAdapter adapter,
			final Index index ) {
		this.adapter = adapter;
		this.index = index;
		dataStore = store;
		duplicateInsertionIds = new HashMap<ByteArray, InsertionIdData>();
		// we need to keep track of the original rows visited so we don't
		// mis-identify them as duplicates
		origRows = new ArrayList<InsertionIdData>();
	}

	@Override
	public void close()
			throws IOException {
		if (closed)
			return;
		else
			closed = true;
		// make sure we take out the original rows first if they accidentally
		// got added to the duplicates
		for (int i = 0; i < origRows.size(); i++) {
			InsertionIdData rowId = origRows.get(i);
			if (duplicateInsertionIds.containsKey(rowId.partitionKey)) {
				duplicateInsertionIds.remove(rowId.partitionKey);
			}
		}

		for (Map.Entry<ByteArray, InsertionIdData> entry : duplicateInsertionIds.entrySet()) {
			final InsertionIdData insertionId = entry.getValue();
			final InsertionIdQuery constraint = new InsertionIdQuery(
					insertionId.partitionKey,
					insertionId.sortKey,
					insertionId.dataId);
			final Query<T> query = (Query) QueryBuilder.newBuilder().indexName(
					index.getName()).addTypeName(
					adapter.getTypeName()).constraints(
					constraint).build();

			// we don't want the duplicates to try to delete one another
			// recursively over and over so we pass false for this deletion
			dataStore.delete(
					query,
					false);
		}
	}

	@Override
	public void entryDeleted(
			T entry,
			GeoWaveRow... rows ) {
		closed = false;
		for (GeoWaveRow row : rows) {
			final int rowDups = row.getNumberOfDuplicates();
			if (rowDups > 0) {
				final InsertionIds ids = DataStoreUtils.getInsertionIdsForEntry(
						entry,
						adapter,
						index);
				// keep track of the original deleted rows
				origRows.add(new InsertionIdData(
						row.getPartitionKey(),
						row.getSortKey(),
						row.getDataId()));
				for (SinglePartitionInsertionIds insertId : ids.getPartitionKeys()) {
					if (!Arrays.equals(
							insertId.getPartitionKey().getBytes(),
							row.getPartitionKey())) {
						for (ByteArray sortKey : insertId.getSortKeys()) {
							final InsertionIdData insertionId = new InsertionIdData(
									insertId.getPartitionKey().getBytes(),
									sortKey.getBytes(),
									row.getDataId());
							if (!duplicateInsertionIds.containsKey(insertionId.partitionKey)) {
								duplicateInsertionIds.put(
										insertionId.partitionKey,
										insertionId);
							}
						}
					}
				}
			}
		}
	}

	private class InsertionIdData
	{
		public final ByteArray partitionKey;
		public final ByteArray sortKey;
		public final ByteArray dataId;

		public InsertionIdData(
				final byte[] partitionKey,
				final byte[] sortKey,
				final byte[] dataId ) {
			this.partitionKey = new ByteArray(
					partitionKey);
			this.sortKey = new ByteArray(
					sortKey);
			this.dataId = new ByteArray(
					dataId);
		}
	}
}
