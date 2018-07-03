package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;
import mil.nga.giat.geowave.datastore.cassandra.util.KeyspaceStatePool;
import mil.nga.giat.geowave.datastore.cassandra.util.KeyspaceStatePool.KeyspaceState;
import mil.nga.giat.geowave.datastore.cassandra.util.SessionPool;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStoreOperations;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class CassandraOperations implements
		MapReduceDataStoreOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CassandraOperations.class);
	protected static final String PRIMARY_ID_KEY = "I";
	protected static final String SECONDARY_ID_KEY = "S";
	// serves as unique ID for instances where primary+secondary are repeated
	protected static final String TIMESTAMP_ID_KEY = "T";
	protected static final String VALUE_KEY = "V";
	private final Session session;
	private final String gwNamespace;
	private final static int WRITE_RESPONSE_THREAD_SIZE = 16;
	private final static int READ_RESPONSE_THREAD_SIZE = 16;
	protected final static ExecutorService WRITE_RESPONSE_THREADS = MoreExecutors
			.getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(WRITE_RESPONSE_THREAD_SIZE));
	protected final static ExecutorService READ_RESPONSE_THREADS = MoreExecutors
			.getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(READ_RESPONSE_THREAD_SIZE));
	private static final Object CREATE_TABLE_MUTEX = new Object();
	private final CassandraOptions options;
	private final KeyspaceState state;

	public CassandraOperations(
			final CassandraRequiredOptions options ) {
		if ((options.getGeowaveNamespace() == null) || options.getGeowaveNamespace().equals(
				"")) {
			gwNamespace = "default";
		}
		else {
			gwNamespace = options.getGeowaveNamespace();
		}
		session = SessionPool.getInstance().getSession(
				options.getContactPoint());
		state = KeyspaceStatePool.getInstance().getCachedState(
				options.getContactPoint(),
				gwNamespace);
		this.options = (CassandraOptions) options.getStoreOptions();
		initKeyspace();
	}

	public void initKeyspace() {
		// TODO consider exposing important keyspace options through commandline
		// such as understanding how to properly enable cassandra in production
		// - with data centers and snitch, for now because this is only creating
		// a keyspace "if not exists" a user can create a keyspace matching
		// their geowave namespace with any settings they want manually
		session.execute(SchemaBuilder.createKeyspace(
				gwNamespace).ifNotExists().with().replication(
				ImmutableMap.of(
						"class",
						"SimpleStrategy",
						"replication_factor",
						options.getReplicationFactor())).durableWrites(
				options.isDurableWrites()));
	}

	public Session getSession() {
		return session;
	}

	public Create getCreateTable(
			final String table ) {
		return SchemaBuilder.createTable(
				gwNamespace,
				table).ifNotExists();
	}

	public void executeCreateTable(
			final Create create,
			final String tableName ) {
		session.execute(create);
		state.tableExistsCache.put(
				tableName,
				true);
	}

	public Insert getInsert(
			final String table ) {
		return QueryBuilder.insertInto(
				gwNamespace,
				table);
	}

	public Delete getDelete(
			final String table ) {
		return QueryBuilder.delete().from(
				gwNamespace,
				table);
	}

	public Select getSelect(
			final String table,
			final String... columns ) {
		return (columns.length == 0 ? QueryBuilder.select() : QueryBuilder.select(columns)).from(
				gwNamespace,
				table);
	}

	public BaseDataStoreOptions getOptions() {
		return options;
	}

	public BatchedWrite getBatchedWrite(
			final String tableName ) {
		PreparedStatement preparedWrite;
		synchronized (state.preparedWritesPerTable) {
			preparedWrite = state.preparedWritesPerTable.get(tableName);
			if (preparedWrite == null) {
				final Insert insert = getInsert(tableName);
				for (final CassandraField f : CassandraField.values()) {
					insert.value(
							f.getFieldName(),
							QueryBuilder.bindMarker(f.getBindMarkerName()));
				}
				preparedWrite = session.prepare(insert);
				state.preparedWritesPerTable.put(
						tableName,
						preparedWrite);
			}
		}
		return new BatchedWrite(
				session,
				preparedWrite,
				options.getBatchWriteSize());
	}

	public BatchedRangeRead getBatchedRangeRead(
			final String tableName,
			final Collection<Short> adapterIds,
			final Collection<SinglePartitionQueryRanges> ranges ) {
		PreparedStatement preparedRead;
		synchronized (state.preparedRangeReadsPerTable) {
			preparedRead = state.preparedRangeReadsPerTable.get(tableName);
			if (preparedRead == null) {
				final Select select = getSelect(tableName);
				select
						.where(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_PARTITION_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.in(
										CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_ADAPTER_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.gte(
										CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_SORT_KEY
												.getLowerBoundBindMarkerName())))
						.and(
								QueryBuilder.lt(
										CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_SORT_KEY
												.getUpperBoundBindMarkerName())));
				preparedRead = session.prepare(select);
				state.preparedRangeReadsPerTable.put(
						tableName,
						preparedRead);
			}
		}

		return new BatchedRangeRead(
				preparedRead,
				this,
				adapterIds,
				ranges);
	}

	public RowRead getRowRead(
			final String tableName,
			final byte[] partitionKey,
			final byte[] sortKey,
			final Short internalAdapterId ) {
		PreparedStatement preparedRead;
		synchronized (state.preparedRowReadPerTable) {
			preparedRead = state.preparedRowReadPerTable.get(tableName);
			if (preparedRead == null) {
				final Select select = getSelect(tableName);
				select
						.where(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_PARTITION_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.in(
										CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_ADAPTER_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_SORT_KEY
												.getBindMarkerName())));
				preparedRead = session.prepare(select);
				state.preparedRowReadPerTable.put(
						tableName,
						preparedRead);
			}
		}

		return new RowRead(
				preparedRead,
				this,
				partitionKey,
				sortKey,
				internalAdapterId == null ? null : internalAdapterId);

	}

	final Semaphore semaphore = new Semaphore(
			1000);

	public CloseableIterator<CassandraRow> executeQueryAsync(
			final Statement... statements ) {
		// first create a list of asynchronous query executions
		final List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(
				statements.length);
		for (final Statement s : statements) {
			try {
				semaphore.acquire();
				try {
					final ResultSetFuture f = session.executeAsync(
							s);
					futures.add(
							f);
					Futures.addCallback(
							f,
							new QueryCallback(semaphore),
							CassandraOperations.READ_RESPONSE_THREADS);
				} catch (Exception e) {
					semaphore.release();
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		Iterator<CassandraRow> resultsIter = 
				Iterators.concat(
						Iterators.transform(futures.iterator(),
								resultsFuture -> Iterators.transform(resultsFuture.getUninterruptibly().iterator(),
										row -> new CassandraRow(row))));
		
		return new CloseableIteratorWrapper<CassandraRow>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (ResultSetFuture f : futures) {
							f.cancel(true);
						}
					}
				},
				resultsIter);
	}

	public CloseableIterator<CassandraRow> executeQuery(
			final Statement... statements ) {
		Iterator<Iterator<Row>> results = Iterators.transform(
				Arrays.asList(statements).iterator(), s -> session.execute(s).iterator());
		Iterator<Row> rows = Iterators.concat(results);
		return new CloseableIterator.Wrapper<CassandraRow>(
				Iterators.transform(rows, r -> new CassandraRow(r)));
	}

	@Override
	public void deleteAll()
			throws Exception {
		state.tableExistsCache.clear();
		session.execute(SchemaBuilder.dropKeyspace(
				gwNamespace).ifExists());
	}

	public boolean deleteAll(
			final String tableName,
			final byte[] adapterId,
			final String... additionalAuthorizations ) {
		// TODO does this actually work? It seems to violate Cassandra rules of
		// always including at least Hash keys on where clause
		session.execute(QueryBuilder.delete().from(
				gwNamespace,
				tableName).where(
				QueryBuilder.eq(
						CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
						ByteBuffer.wrap(adapterId))));
		return true;
	}

	public boolean deleteRows(
			final String tableName,
			final byte[][] dataIds,
			final short internalAdapterId,
			final String... additionalAuthorizations ) {
		session.execute(QueryBuilder.delete().from(
				gwNamespace,
				tableName).where(
				QueryBuilder.eq(
						CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
						internalAdapterId)).and(
				QueryBuilder.in(
						CassandraField.GW_DATA_ID_KEY.getFieldName(),
						Lists.transform(
								Arrays.asList(dataIds),
								new ByteArrayToByteBuffer()))));
		return true;
	}

	public CloseableIterator<CassandraRow> getRows(
			final String tableName,
			final byte[][] dataIds,
			final Short internalAdapterId,
			final String... additionalAuthorizations ) {
		final Set<ByteArrayId> dataIdsSet = new HashSet<ByteArrayId>(
				dataIds.length);
		for (int i = 0; i < dataIds.length; i++) {
			dataIdsSet.add(new ByteArrayId(
					dataIds[i]));
		}
		final CloseableIterator<CassandraRow> everything = executeQuery(QueryBuilder.select().from(
				gwNamespace,
				tableName).allowFiltering());
		return new CloseableIteratorWrapper<CassandraRow>(
				everything,
				Iterators.filter(
						everything,
						new Predicate<GeoWaveRow>() {

							@Override
							public boolean apply(
									final GeoWaveRow input ) {
								return dataIdsSet.contains(new ByteArrayId(
										input.getDataId())) && (input.getInternalAdapterId() == internalAdapterId);
							}
						}));
	}

	public boolean deleteRow(
			final String tableName,
			final GeoWaveRow row,
			final String... additionalAuthorizations ) {
		boolean exhausted = true;
		for (int i = 0; i < row.getFieldValues().length; i++) {
			final ResultSet rs = session.execute(QueryBuilder.delete().from(
					gwNamespace,
					tableName).where(
					QueryBuilder.eq(
							CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
							ByteBuffer.wrap(row.getPartitionKey()))).and(
					QueryBuilder.eq(
							CassandraField.GW_SORT_KEY.getFieldName(),
							ByteBuffer.wrap(row.getSortKey()))).and(
					QueryBuilder.eq(
							CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
							row.getInternalAdapterId())).and(
					QueryBuilder.eq(
							CassandraField.GW_DATA_ID_KEY.getFieldName(),
							ByteBuffer.wrap(row.getDataId()))).and(
					QueryBuilder.eq(
							CassandraField.GW_FIELD_VISIBILITY_KEY.getFieldName(),
							ByteBuffer.wrap(row.getFieldValues()[i].getVisibility()))));
			exhausted &= rs.isExhausted();
		}

		return !exhausted;
	}

	private static class ByteArrayToByteBuffer implements
			Function<byte[], ByteBuffer>
	{
		@Override
		public ByteBuffer apply(
				final byte[] input ) {
			return ByteBuffer.wrap(input);
		}
	};

	public static class ByteArrayIdToByteBuffer implements
			Function<ByteArrayId, ByteBuffer>
	{
		@Override
		public ByteBuffer apply(
				final ByteArrayId input ) {
			return ByteBuffer.wrap(input.getBytes());
		}
	}

	public static class StringToByteBuffer implements
			Function<String, ByteBuffer>
	{
		@Override
		public ByteBuffer apply(
				final String input ) {
			return ByteBuffer.wrap(StringUtils.stringToBinary(input));
		}
	}

	// callback class
	protected static class QueryCallback implements
			FutureCallback<ResultSet>
	{
		private final Semaphore semaphore;

		public QueryCallback(
				Semaphore semaphore ) {
			this.semaphore = semaphore;
		}

		@Override
		public void onSuccess(
				final ResultSet result ) {
			// placeholder: put any logging or on success logic here.
			semaphore.release();
		}

		@Override
		public void onFailure(
				final Throwable t ) {
			// go ahead and wrap in a runtime exception for this case, but you
			// can do logging or start counting errors.
			t.printStackTrace();
			semaphore.release();
			throw new RuntimeException(
					t);
		}
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException {
		final String tableName = indexId.getString();
		Boolean tableExists = state.tableExistsCache.get(tableName);
		if (tableExists == null) {
			final KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(
					gwNamespace);
			if (keyspace != null) {
				tableExists = keyspace.getTable(tableName) != null;
			}
			else {
				tableExists = false;
			}
			state.tableExistsCache.put(
					tableName,
					tableExists);
		}
		return tableExists;
	}

	@Override
	public boolean deleteAll(
			final ByteArrayId indexId,
			final Short internalAdapterId,
			final String... additionalAuthorizations ) {
		return false;
	}

	@Override
	public boolean insureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	@Override
	public Writer createWriter(
			final ByteArrayId indexId,
			final short internalAdapterId ) {
		if (options.isCreateTable()) {
			synchronized (CREATE_TABLE_MUTEX) {
				try {
					if (!indexExists(indexId)) {
						final String tableName = indexId.getString();
						final Create create = getCreateTable(tableName);
						for (final CassandraField f : CassandraField.values()) {
							f.addColumn(create);
						}
						executeCreateTable(
								create,
								tableName);
					}
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to create table '" + indexId.getString() + "'",
							e);
				}
			}
		}
		return new CassandraWriter(
				indexId.getString(),
				this);
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		final String tableName = getMetadataTableName(metadataType);
		if (options.isCreateTable()) {
			// this checks for existence prior to create
			synchronized (CREATE_TABLE_MUTEX) {
				try {
					if (!indexExists(new ByteArrayId(
							tableName))) {
						// create table
						final Create create = getCreateTable(tableName);
						create.addPartitionKey(
								PRIMARY_ID_KEY,
								DataType.blob());
						if (MetadataType.STATS.equals(metadataType)
								|| MetadataType.INTERNAL_ADAPTER.equals(metadataType)) {
							create.addClusteringColumn(
									SECONDARY_ID_KEY,
									DataType.blob());
							create.addClusteringColumn(
									TIMESTAMP_ID_KEY,
									DataType.timeuuid());
						}
						create.addColumn(
								VALUE_KEY,
								DataType.blob());
						executeCreateTable(
								create,
								tableName);
					}
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to check if table exists",
							e);
				}
			}
		}
		return new CassandraMetadataWriter(
				this,
				tableName);
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new CassandraMetadataReader(
				this,
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new CassandraMetadataDeleter(
				this,
				metadataType);
	}

	@Override
	public Reader createReader(
			final ReaderParams readerParams ) {
		return new CassandraReader(
				readerParams,
				this);
	}

	@Override
	public Deleter createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception {
		return new CassandraDeleter(
				this,
				indexId.getString());
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Reader createReader(
			final RecordReaderParams recordReaderParams ) {
		return new CassandraReader(
				recordReaderParams,
				this);
	}

	@Override
	public boolean metadataExists(
			final MetadataType metadataType )
			throws IOException {
		return indexExists(new ByteArrayId(
				getMetadataTableName(metadataType)));
	}

	public String getMetadataTableName(
			final MetadataType metadataType ) {
		final String tableName = metadataType.name() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
		return tableName;
	}
}
