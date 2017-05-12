package mil.nga.giat.geowave.core.store.memory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.base.DataStoreCallbackManager;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import mil.nga.giat.geowave.core.store.index.writer.IndexCompositeWriter;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class MemoryDataStore implements
		DataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(MemoryDataStore.class);
	private final Map<ByteArrayId, TreeSet<MemoryEntryRow>> storeData = Collections
			.synchronizedMap(new HashMap<ByteArrayId, TreeSet<MemoryEntryRow>>());
	private final AdapterStore adapterStore;
	private final IndexStore indexStore;
	private final DataStatisticsStore statsStore;
	private final SecondaryIndexDataStore secondaryIndexDataStore;
	private final AdapterIndexMappingStore adapterIndexMappingStore;

	public MemoryDataStore() {
		super();
		adapterStore = new MemoryAdapterStore();
		indexStore = new MemoryIndexStore();
		statsStore = new MemoryDataStatisticsStore();
		secondaryIndexDataStore = new MemorySecondaryIndexDataStore();
		adapterIndexMappingStore = new MemoryAdapterIndexMappingStore();
	}

	public MemoryDataStore(
			final AdapterStore adapterStore,
			final IndexStore indexStore,
			final DataStatisticsStore statsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		super();
		this.adapterStore = adapterStore;
		this.indexStore = indexStore;
		this.statsStore = statsStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;
		this.adapterIndexMappingStore = adapterIndexMappingStore;
	}

	@Override
	public <T> IndexWriter createWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex... indices )
			throws MismatchedIndexToAdapterMapping {
		adapterStore.addAdapter(adapter);

		adapterIndexMappingStore.addAdapterIndexMapping(new AdapterToIndexMapping(
				adapter.getAdapterId(),
				indices));
		final IndexWriter<T>[] writers = new IndexWriter[indices.length];
		int i = 0;
		for (final PrimaryIndex index : indices) {
			indexStore.addIndex(index);
			writers[i] = new MyIndexWriter<T>(
					DataStoreUtils.UNCONSTRAINED_VISIBILITY,
					adapter,
					index,
					i == 0);
			if (adapter instanceof IndexDependentDataAdapter) {
				writers[i] = new IndependentAdapterIndexWriter<T>(
						(IndexDependentDataAdapter<T>) adapter,
						index,
						writers[i]);
			}
			i++;
		}
		return new IndexCompositeWriter(
				writers);
	}

	private class MyIndexWriter<T> implements
			IndexWriter<T>
	{
		final PrimaryIndex index;
		final DataAdapter<T> adapter;
		final VisibilityWriter<T> customFieldVisibilityWriter;
		final DataStoreCallbackManager callbackCache;

		public MyIndexWriter(
				final VisibilityWriter<T> customFieldVisibilityWriter,
				final DataAdapter<T> adapter,
				final PrimaryIndex index,
				final boolean captureAdapterStats ) {
			super();
			this.index = index;
			this.adapter = adapter;
			this.customFieldVisibilityWriter = customFieldVisibilityWriter;
			callbackCache = new DataStoreCallbackManager(
					statsStore,
					secondaryIndexDataStore,
					captureAdapterStats);
		}

		@Override
		public void close()
				throws IOException {
			callbackCache.close();
		}

		@Override
		public List<ByteArrayId> write(
				final T entry ) {
			return write(
					entry,
					customFieldVisibilityWriter);
		}

		@Override
		public List<ByteArrayId> write(
				final T entry,
				final VisibilityWriter<T> fieldVisibilityWriter ) {
			final List<ByteArrayId> ids = new ArrayList<ByteArrayId>();

			final IngestCallback<T> callback = callbackCache.getIngestCallback(
					(WritableDataAdapter) this.adapter,
					index);

			final List<MemoryEntryRow> rows = MemoryStoreUtils.entryToRows(
					(WritableDataAdapter) this.adapter,
					index,
					entry,
					callback,
					fieldVisibilityWriter);
			for (final MemoryEntryRow row : rows) {
				ids.add(row.getRowId());
				final TreeSet<MemoryEntryRow> rowTreeSet = getRowsForIndex(index.getId());
				if (rowTreeSet.contains(row)) {
					rowTreeSet.remove(row);
				}
				if (!rowTreeSet.add(row)) {
					LOGGER.warn("Unable to add new entry");
				}
			}

			return ids;
		}

		@Override
		public PrimaryIndex[] getIndices() {
			return new PrimaryIndex[] {
				index
			};
		}

		@Override
		public void flush() {
			try {
				close();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Error closing index writer",
						e);
			}
		}

	}

	private TreeSet<MemoryEntryRow> getRowsForIndex(
			final ByteArrayId id ) {
		TreeSet<MemoryEntryRow> set = storeData.get(id);
		if (set == null) {
			set = new TreeSet<MemoryEntryRow>();
			storeData.put(
					id,
					set);
		}
		return set;
	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {

		try (CloseableIterator<?> it = query(
				queryOptions,
				query,
				true)) {
			while (it.hasNext()) {
				it.next();
				it.remove();
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Failed deletetion",
					e);
			return false;
		}

		return true;
	}

	/**
	 * Returns all data in this data store that matches the query parameter
	 * within the index described by the index passed in and matches the adapter
	 * (the same adapter ID as the ID ingested). All data that matches the
	 * query, adapter ID, and is in the index ID will be returned as an instance
	 * of the native data type that this adapter supports. The iterator will
	 * only return as many results as the limit passed in.
	 *
	 * @param queryOptions
	 *            additional options for the processing the query
	 * @param the
	 *            data constraints for the query
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	@Override
	public <T> CloseableIterator<T> query(
			final QueryOptions queryOptions,
			final Query query ) {
		return query(
				queryOptions,
				query,
				false);
	}

	private CloseableIterator query(
			final QueryOptions queryOptions,
			final Query query,
			final boolean isDelete ) {
		final DedupeFilter filter = new DedupeFilter();
		filter.setDedupAcrossIndices(false);
		try {
			// keep a list of adapters that have been queried, to only low an
			// adapter to be queried
			// once
			final Set<ByteArrayId> queriedAdapters = new HashSet<ByteArrayId>();

			final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();

			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : queryOptions
					.getIndicesForAdapters(
							adapterStore,
							adapterIndexMappingStore,
							indexStore)) {
				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

					final boolean firstTimeForAdapter = queriedAdapters.add(adapter.getAdapterId());
					if (!(firstTimeForAdapter || isDelete)) {
						continue;
					}

					final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
							statsStore,
							secondaryIndexDataStore,
							firstTimeForAdapter);

					populateResults(
							results,
							adapter,
							indexAdapterPair.getLeft(),
							query,
							isDelete ? null : filter,
							queryOptions,
							isDelete,
							callbackManager);
				}
			}
			return new CloseableIteratorWrapper(
					new Closeable() {
						@Override
						public void close()
								throws IOException {
							for (final CloseableIterator<?> result : results) {
								result.close();
							}
						}
					},
					Iterators.concat(results.iterator()),
					queryOptions.getLimit());

		}
		catch (final IOException e)

		{
			LOGGER.error(
					"Cannot process query [" + (query == null ? "all" : query.toString()) + "]",
					e);
			return new CloseableIterator.Empty();
		}

	}

	private void populateResults(
			final List<CloseableIterator<Object>> results,
			final DataAdapter<Object> adapter,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter filter,
			final QueryOptions queryOptions,
			final boolean isDelete,
			final DataStoreCallbackManager callbackCache ) {
		final TreeSet<MemoryEntryRow> set = getRowsForIndex(index.getId());
		final Iterator<MemoryEntryRow> rowIt = ((TreeSet<MemoryEntryRow>) set.clone()).iterator();
		final List<QueryFilter> filters = (query == null) ? new ArrayList<QueryFilter>() : new ArrayList<QueryFilter>(
				query.createFilters(index.getIndexModel()));
		filters.add(new QueryFilter() {
			@Override
			public boolean accept(
					final CommonIndexModel indexModel,
					final IndexedPersistenceEncoding persistenceEncoding ) {
				if (adapter.getAdapterId().equals(
						persistenceEncoding.getAdapterId())) {
					return true;
				}
				return false;
			}
		});
		if (filter != null) {
			filters.add(filter);
		}

		results.add(new CloseableIterator() {
			MemoryEntryRow nextRow = null;
			MemoryEntryRow currentRow = null;
			IndexedAdapterPersistenceEncoding encoding = null;

			private boolean getNext() {
				while ((nextRow == null) && rowIt.hasNext()) {
					final MemoryEntryRow row = rowIt.next();
					final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
							row.getTableRowId().getAdapterId()));
					encoding = MemoryStoreUtils.getEncoding(
							index.getIndexModel(),
							adapter,
							row);
					boolean ok = true;
					for (final QueryFilter filter : filters) {
						if (!filter.accept(
								index.getIndexModel(),
								encoding)) {
							ok = false;
							break;
						}
					}
					ok &= isAuthorized(
							row,
							queryOptions.getAuthorizations());
					if (ok) {
						nextRow = row;
						break;
					}
				}
				return (nextRow != null);
			}

			@Override
			public boolean hasNext() {
				return getNext();
			}

			@Override
			public Object next() {
				currentRow = nextRow;
				if (isDelete) {
					final DataAdapter adapter = adapterStore.getAdapter(encoding.getAdapterId());
					if (adapter instanceof WritableDataAdapter) {
						callbackCache.getDeleteCallback(
								(WritableDataAdapter) adapter,
								index).entryDeleted(
								currentRow.getInfo(),
								currentRow.entry);
					}
				}
				((ScanCallback) queryOptions.getScanCallback()).entryScanned(
						currentRow.getInfo(),
						currentRow.entry);
				nextRow = null;
				return currentRow.entry;
			}

			@Override
			public void remove() {
				if (currentRow != null) {
					set.remove(currentRow);
				}
			}

			@Override
			public void close()
					throws IOException {
				final ScanCallback<?> callback = queryOptions.getScanCallback();
				if ((callback != null) && (callback instanceof Closeable)) {
					((Closeable) callback).close();
				}
			}
		});
		boolean isAggregation = (queryOptions.getAggregation() != null);
		if (isAggregation) {
			Aggregation agg = queryOptions.getAggregation().getRight();
			for (CloseableIterator r : results) {
				while (r.hasNext()) {
					Object entry = r.next();
					if (agg instanceof CommonIndexAggregation) {
						agg.aggregate(adapter.encode(
								entry,
								index.getIndexModel()));
					}
					else {
						agg.aggregate(entry);
					}
				}
				try {
					r.close();
				}
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			results.clear();
			results.add(new CloseableIterator.Wrapper<>(
					Iterators.singletonIterator((Object) agg.getResult())));
		}
	}

	protected static IndexedAdapterPersistenceEncoding getEncoding(
			final CommonIndexModel model,
			final DataAdapter<?> adapter,
			final MemoryEntryRow row ) {
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		for (final FieldInfo column : row.info.getFieldInfo()) {
			final FieldReader<? extends CommonIndexValue> reader = model.getReader(column.getDataValue().getId());
			if (reader == null) {
				final FieldReader extendedReader = adapter.getReader(column.getDataValue().getId());
				if (extendedReader != null) {
					extendedData.addValue(column.getDataValue());
				}
				else {
					unknownData.addValue(new PersistentValue<byte[]>(
							column.getDataValue().getId(),
							column.getWrittenValue()));
				}
			}
			else {
				commonData.addValue(column.getDataValue());
			}
		}
		return new IndexedAdapterPersistenceEncoding(
				new ByteArrayId(
						row.getTableRowId().getAdapterId()),
				new ByteArrayId(
						row.getTableRowId().getDataId()),
				new ByteArrayId(
						row.getTableRowId().getInsertionId()),
				row.getTableRowId().getNumberOfDuplicates(),
				commonData,
				unknownData,
				extendedData);
	}

	private boolean isAuthorized(
			final MemoryEntryRow row,
			final String... authorizations ) {
		for (final FieldInfo info : row.info.getFieldInfo()) {
			if (!MemoryStoreUtils.isAuthorized(
					info.getVisibility(),
					authorizations)) {
				return false;
			}
		}
		return true;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public DataStatisticsStore getStatsStore() {
		return statsStore;
	}
}