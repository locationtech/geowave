package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.StreamSupport;

import com.aol.cyclops.control.LazyReact;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;
import mil.nga.giat.geowave.datastore.cassandra.CassandraWriter;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;
import mil.nga.giat.geowave.datastore.cassandra.util.KeyspaceStatePool;
import mil.nga.giat.geowave.datastore.cassandra.util.KeyspaceStatePool.KeyspaceState;
import mil.nga.giat.geowave.datastore.cassandra.util.SessionPool;

public class CassandraOperations implements
		DataStoreOperations
{
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
		this.options = options.getAdditionalOptions();
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

	@Override
	public boolean tableExists(
			final String tableName ) {
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
			final List<ByteArrayId> adapterIds,
			final List<ByteArrayRange> ranges ) {
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
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_IDX_KEY
												.getLowerBoundBindMarkerName())))
						.and(
								QueryBuilder.lt(
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_IDX_KEY
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

	public BatchedRangeRead getBatchedRangeRead(
			final String tableName,
			final List<ByteArrayId> adapterIds ) {
		return getBatchedRangeRead(
				tableName,
				adapterIds,
				new ArrayList<>());
	}

	public RowRead getRowRead(
			final String tableName,
			final byte[] rowIdx,
			final ByteArrayId adapterId ) {
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
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_IDX_KEY
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
				rowIdx,
				adapterId == null ? null : adapterId.getBytes());

	}

	public RowRead getRowRead(
			final String tableName ) {
		return getRowRead(
				tableName,
				null,
				null);
	}

	public CloseableIterator<CassandraRow> executeQueryAsync(
			final Statement... statements ) {
		// first create a list of asynchronous query executions
		final List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(
				statements.length);
		for (final Statement s : statements) {
			final ResultSetFuture f = session.executeAsync(
					s);
			futures.add(
					f);
			Futures.addCallback(
					f,
					new QueryCallback(),
					CassandraOperations.READ_RESPONSE_THREADS);
		}
		// convert the list of futures to an asynchronously as completed
		// iterator on cassandra rows
		final com.aol.cyclops.internal.react.stream.CloseableIterator<CassandraRow> results = new LazyReact()
				.fromStreamFutures(
						Lists.transform(
								futures,
								new ListenableFutureToCompletableFuture()).stream())
				.flatMap(
						r -> StreamSupport.stream(
								r.spliterator(),
								false))
				.map(
						r -> new CassandraRow(
								r))
				.iterator();
		// now convert cyclops-react closeable iterator to a geowave closeable
		// iterator
		return new CloseableIteratorWrapper<CassandraRow>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						results.close();
					}
				},
				results);
	}

	public CloseableIterator<CassandraRow> executeQuery(
			final Statement... statements ) {
		final List<CassandraRow> rows = new ArrayList<>();
		for (final Statement s : statements) {
			final ResultSet r = session.execute(s);
			for (final Row row : r) {
				rows.add(new CassandraRow(
						row));
			}
		}
		return new CloseableIterator.Wrapper<CassandraRow>(
				rows.iterator());
	}

	public Writer createWriter(
			final String tableName,
			final boolean createTable ) {
		final CassandraWriter writer = new CassandraWriter(
				tableName,
				this);
		if (createTable) {
			synchronized (CREATE_TABLE_MUTEX) {
				if (!tableExists(tableName)) {
					final Create create = getCreateTable(tableName);
					for (final CassandraField f : CassandraField.values()) {
						f.addColumn(create);
					}
					executeCreateTable(
							create,
							tableName);
				}
			}
		}
		return writer;
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
			final byte[] adapterId,
			final String... additionalAuthorizations ) {
		session.execute(QueryBuilder.delete().from(
				gwNamespace,
				tableName).where(
				QueryBuilder.eq(
						CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
						ByteBuffer.wrap(adapterId))).and(
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
			final byte[] adapterId,
			final String... additionalAuthorizations ) {
		final ByteArrayId adapterIdObj = new ByteArrayId(
				adapterId);
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
						new Predicate<CassandraRow>() {

							@Override
							public boolean apply(
									final CassandraRow input ) {
								return dataIdsSet.contains(new ByteArrayId(
										input.getDataId())) && new ByteArrayId(
										input.getAdapterId()).equals(adapterIdObj);
							}
						}));
	}

	public boolean deleteRow(
			final String tableName,
			final CassandraRow row,
			final String... additionalAuthorizations ) {
		session.execute(QueryBuilder.delete().from(
				gwNamespace,
				tableName).where(
				QueryBuilder.eq(
						CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
						ByteBuffer.wrap(row.getPartitionId()))).and(
				QueryBuilder.eq(
						CassandraField.GW_IDX_KEY.getFieldName(),
						ByteBuffer.wrap(row.getIndex()))).and(
				QueryBuilder.eq(
						CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
						ByteBuffer.wrap(row.getAdapterId()))));

		return true;
	}

	private static class ListenableFutureToCompletableFuture implements
			Function<ListenableFuture<ResultSet>, CompletableFuture<ResultSet>>
	{
		@Override
		public CompletableFuture<ResultSet> apply(
				final ListenableFuture<ResultSet> input ) {
			return CompletableFuturesExtra.toCompletableFuture(input);
		}
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

	// callback class
	protected static class QueryCallback implements
			FutureCallback<ResultSet>
	{

		@Override
		public void onSuccess(
				final ResultSet result ) {
			// placeholder: put any logging or on success logic here.
		}

		@Override
		public void onFailure(
				final Throwable t ) {
			// go ahead and wrap in a runtime exception for this case, but you
			// can do logging or start counting errors.
			t.printStackTrace();
			throw new RuntimeException(
					t);
		}
	}
}
