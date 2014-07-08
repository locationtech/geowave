package mil.nga.giat.geowave.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.accumulo.MutationIteratorWrapper.EntryToMutationConverter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.IngestCallback;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.query.Query;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

/**
 * This is the Accumulo implementation of the data store. It requires an
 * AccumuloOperations instance that describes how to connect (read/write data)
 * to Apache Accumulo. It can create default implementations of the IndexStore
 * and AdapterStore based on the operations which will persist configuration
 * information to Accumulo tables, or an implementation of each of these stores
 * can be passed in.
 * 
 * A DataStore can both ingest and query data based on persisted indices and
 * data adapters. When the data is ingested it is explicitly given an index and
 * a data adapter which is then persisted to be used in subsequent queries.
 */
public class AccumuloDataStore implements
		DataStore
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStore.class);

	protected final IndexStore indexStore;
	protected final AdapterStore adapterStore;
	protected final AccumuloOperations accumuloOperations;

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations ) {
		this(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				accumuloOperations);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final AccumuloOperations accumuloOperations ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.accumuloOperations = accumuloOperations;
	}

	@Override
	public <T> IndexWriter createIndexWriter(
			final Index index ) {
		return new AccumuloIndexWriter(
				index,
				accumuloOperations,
				this);
	}

	@Override
	public <T> List<ByteArrayId> ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry ) {
		store(writableAdapter);
		store(index);
		Writer writer;
		try {
			writer = accumuloOperations.createWriter(StringUtils.stringFromBinary(index.getId().getBytes()));
			final List<ByteArrayId> rowIds = AccumuloUtils.write(
					writableAdapter,
					index,
					entry,
					writer);

			writer.close();
			return rowIds;
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to ingest data entry",
					e);
		}
		return new ArrayList<ByteArrayId>();
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (!adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
	}

	protected synchronized void store(
			final Index index ) {
		if (!indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}

	@Override
	public <T> void ingest(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator ) {
		ingest(
				dataWriter,
				index,
				entryIterator,
				null);
	}

	@Override
	public <T> void ingest(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallBack ) {
		try {
			store(dataWriter);
			store(index);

			final mil.nga.giat.geowave.accumulo.Writer writer = accumuloOperations.createWriter(StringUtils.stringFromBinary(index.getId().getBytes()));

			writer.write(new Iterable<Mutation>() {
				@Override
				public Iterator<Mutation> iterator() {
					return new MutationIteratorWrapper<T>(
							entryIterator,
							new EntryToMutationConverter<T>() {

								@Override
								public List<Mutation> convert(
										final T entry ) {
									return AccumuloUtils.entryToMutation(
											dataWriter,
											index,
											entry);
								}
							},
							ingestCallBack);
				}
			});
			writer.close();
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to ingest data entries",
					e);
		}
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Query query ) {
		return query(
				adapter,
				query,
				null);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Query query,
			final int limit ) {
		return query(
				adapter,
				query,
				new Integer(
						limit));
	}

	@SuppressWarnings("unchecked")
	private <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Query query,
			final Integer limit ) {
		if (!adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
		return ((CloseableIterator<T>) query(
				Arrays.asList(new ByteArrayId[] {
					adapter.getAdapterId()
				}),
				query,
				new MemoryAdapterStore(
						new DataAdapter[] {
							adapter
						}),
				limit));
	}

	@Override
	public CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query,
			final int limit ) {
		return query(
				adapterIds,
				query,
				adapterStore,
				limit);
	}

	@Override
	public CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query ) {
		return query(
				adapterIds,
				query,
				adapterStore,
				null);
	}

	private CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query,
			final AdapterStore adapterStore,
			final Integer limit ) {
		// query the indices that are supported for this query object, and these
		// data adapter Ids
		final Iterator<Index> indices = indexStore.getIndices();
		final List<CloseableIterator<?>> results = new ArrayList<CloseableIterator<?>>();
		while (indices.hasNext()) {
			final Index index = indices.next();
			final AccumuloConstraintsQuery accumuloQuery;
			if (query == null) {
				accumuloQuery = new AccumuloConstraintsQuery(
						adapterIds,
						index);
			}
			else if (query.isSupported(index)) {
				// construct the query
				accumuloQuery = new AccumuloConstraintsQuery(
						adapterIds,
						index,
						query.getIndexConstraints(index.getIndexStrategy()),
						query.createFilters(index.getIndexModel()));
			}
			else {
				continue;
			}
			results.add(accumuloQuery.query(
					accumuloOperations,
					adapterStore,
					limit));
		}
		// concatenate iterators
		return new CloseableIteratorWrapper<Object>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<?> result : results) {
							result.close();
						}
					}
				},
				Iterators.concat(results.iterator()));
	}

	protected static byte[] getRowIdBytes(
			final AccumuloRowId rowElements ) {
		final ByteBuffer buf = ByteBuffer.allocate(12 + rowElements.getDataId().length + rowElements.getAdapterId().length + rowElements.getIndexId().length);
		buf.put(rowElements.getIndexId());
		buf.put(rowElements.getAdapterId());
		buf.put(rowElements.getDataId());
		buf.putInt(rowElements.getAdapterId().length);
		buf.putInt(rowElements.getDataId().length);
		buf.putInt(rowElements.getNumberOfDuplicates());
		return buf.array();
	}

	protected static AccumuloRowId getRowIdObject(
			final byte[] row ) {
		final byte[] metadata = Arrays.copyOfRange(
				row,
				row.length - 12,
				row.length);
		final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				row,
				0,
				row.length - 12);
		final byte[] indexId = new byte[row.length - 12 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		buf.get(indexId);
		buf.get(adapterId);
		buf.get(dataId);
		return new AccumuloRowId(
				indexId,
				dataId,
				adapterId,
				numberOfDuplicates);
	}

	@Override
	public <T> T getEntry(
			final Index index,
			final ByteArrayId rowId ) {
		final AccumuloRowIdQuery q = new AccumuloRowIdQuery(
				index,
				rowId);
		return (T) q.query(
				accumuloOperations,
				adapterStore);
	}

	@Override
	public CloseableIterator<?> getEntriesByPrefix(
			final Index index,
			final ByteArrayId rowPrefix ) {
		final AccumuloRowPrefixQuery q = new AccumuloRowPrefixQuery(
				index,
				rowPrefix);
		return q.query(
				accumuloOperations,
				adapterStore);
	}

	@Override
	public CloseableIterator<?> query(
			final Query query ) {
		return query(
				(List<ByteArrayId>) null,
				query);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final Index index,
			final Query query ) {
		return query(
				index,
				query,
				null);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final Index index,
			final Query query,
			final int limit ) {
		return query(
				index,
				query,
				(Integer) limit);
	}

	@Override
	public CloseableIterator<?> query(
			final Query query,
			final int limit ) {
		return query(
				(List<ByteArrayId>) null,
				query,
				limit);
	}

	@SuppressWarnings("unchecked")
	private <T> CloseableIterator<T> query(
			final Index index,
			final Query query,
			final Integer limit ) {
		if (!query.isSupported(index)) {
			throw new IllegalArgumentException(
					"Index does not support the query");
		}
		final AccumuloConstraintsQuery q = new AccumuloConstraintsQuery(
				index,
				query.getIndexConstraints(index.getIndexStrategy()),
				query.createFilters(index.getIndexModel()));
		return (CloseableIterator<T>) q.query(
				accumuloOperations,
				adapterStore,
				limit);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final int limit ) {
		return query(
				adapter,
				index,
				query,
				(Integer) limit);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query ) {
		return query(
				adapter,
				index,
				query,
				null);
	}

	@SuppressWarnings("unchecked")
	private <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final Integer limit ) {
		if (!query.isSupported(index)) {
			throw new IllegalArgumentException(
					"Index does not support the query");
		}
		if (!adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}

		final AccumuloConstraintsQuery q = new AccumuloConstraintsQuery(
				Arrays.asList(new ByteArrayId[] {
					adapter.getAdapterId()
				}),
				index,
				query.getIndexConstraints(index.getIndexStrategy()),
				query.createFilters(index.getIndexModel()));
		return (CloseableIterator<T>) q.query(
				accumuloOperations,
				new MemoryAdapterStore(
						new DataAdapter[] {
							adapter
						}),
				limit);
	}
}
