package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

public class AccumuloConstraintsDelete extends
		AccumuloConstraintsQuery
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloConstraintsDelete.class);

	public AccumuloConstraintsDelete(
			List<ByteArrayId> adapterIds,
			PrimaryIndex index,
			Query query,
			DedupeFilter clientDedupeFilter,
			ScanCallback<?> scanCallback,
			Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			IndexMetaData[] indexMetaData,
			DuplicateEntryCount duplicateCounts,
			DifferingFieldVisibilityEntryCount visibilityCounts,
			String[] authorizations ) {
		super(
				adapterIds,
				index,
				query,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				fieldIdsAdapterPair,
				indexMetaData,
				duplicateCounts,
				visibilityCounts,
				authorizations);
	}

	public AccumuloConstraintsDelete(
			List<ByteArrayId> adapterIds,
			PrimaryIndex index,
			List<MultiDimensionalNumericData> constraints,
			List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			ScanCallback<?> scanCallback,
			Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			IndexMetaData[] indexMetaData,
			DuplicateEntryCount duplicateCounts,
			DifferingFieldVisibilityEntryCount visibilityCounts,
			String[] authorizations ) {
		super(
				adapterIds,
				index,
				constraints,
				queryFilters,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				fieldIdsAdapterPair,
				indexMetaData,
				duplicateCounts,
				visibilityCounts,
				authorizations);
	}

	@Override
	protected CloseableIterator<Object> initCloseableIterator(
			ScannerBase scanner,
			Iterator it ) {
		return new CloseableIteratorWrapper(
				new Closeable() {
					boolean closed = false;

					@Override
					public void close()
							throws IOException {
						if (!closed) {
							if (scanner instanceof BatchDeleter) {
								try {
									((BatchDeleter) scanner).delete();
								}
								catch (MutationsRejectedException | TableNotFoundException e) {
									LOGGER.warn(
											"Unable to delete rows by query constraints",
											e);
								}
							}
							scanner.close();
						}
						closed = true;
					}
				},
				it);
	}

	@Override
	protected boolean useWholeRowIterator() {
		return false;
	}

	@Override
	protected ScannerBase createScanner(
			AccumuloOperations accumuloOperations,
			String tableName,
			boolean batchScanner,
			String... authorizations )
			throws TableNotFoundException {
		BatchDeleter deleter = accumuloOperations.createBatchDeleter(
				tableName,
				authorizations);
		deleter.removeScanIterator(BatchDeleter.class.getName() + ".NOVALUE");
		return deleter;
	}

}
