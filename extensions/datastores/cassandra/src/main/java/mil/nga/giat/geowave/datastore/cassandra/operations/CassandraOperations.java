package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.StreamSupport;

import com.aol.cyclops.control.LazyReact;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;
import mil.nga.giat.geowave.datastore.cassandra.CassandraWriter;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;
import mil.nga.giat.geowave.datastore.cassandra.util.SessionPool;

public class CassandraOperations implements
		DataStoreOperations
{
	private final Session session;
	private final String gwNamespace;
	private final static int WRITE_RESPONSE_THREAD_SIZE = 16;
	private final static int READ_RESPONSE_THREAD_SIZE = 16;
	protected final static ExecutorService WRITE_RESPONSE_THREADS = MoreExecutors.getExitingExecutorService(
			(ThreadPoolExecutor) Executors.newFixedThreadPool(
					WRITE_RESPONSE_THREAD_SIZE));
	protected final static ExecutorService READ_RESPONSE_THREADS = MoreExecutors.getExitingExecutorService(
			(ThreadPoolExecutor) Executors.newFixedThreadPool(
					READ_RESPONSE_THREAD_SIZE));

	private final Map<String, PreparedStatement> preparedRangeReadsPerTable = new HashMap<>();
	private final Map<String, PreparedStatement> preparedRowReadPerTable = new HashMap<>();
	private static Map<String, Boolean> tableExistsCache = new HashMap<>();

	private final CassandraOptions options;

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
		this.options = options.getAdditionalOptions();
	}

	@Override
	public boolean tableExists(
			final String tableName ) {
		Boolean tableExists = tableExistsCache.get(
				tableName);
		if (tableExists == null) {
			tableExists = session.getCluster().getMetadata().getKeyspace(
					gwNamespace).getTable(
							tableName) != null;
			tableExistsCache.put(
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
				table);
	}

	public void executeCreateTable(
			final Create create,
			final String tableName ) {
		session.execute(
				create);
		tableExistsCache.put(
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
		return QueryBuilder.select(
				columns).from(
						gwNamespace,
						table);
	}

	public BaseDataStoreOptions getOptions() {
		return options;
	}

	public BatchedWrite getBatchedWrite() {
		return new BatchedWrite(
				session,
				options.getBatchWriteSize());
	}

	public BatchedRangeRead getBatchedRangeRead(
			final String tableName,
			final List<ByteArrayRange> ranges ) {
		PreparedStatement preparedRead;
		synchronized (preparedRangeReadsPerTable) {
			preparedRead = preparedRangeReadsPerTable.get(
					tableName);
			if (preparedRead == null) {
				final Select select = getSelect(
						tableName);
				select
						.where(
								QueryBuilder.gte(
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(
												CassandraRow.CassandraField.GW_IDX_KEY.getLowerBoundBindMarkerName())))
						.and(
								QueryBuilder.lt(
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(
												CassandraRow.CassandraField.GW_IDX_KEY.getUpperBoundBindMarkerName())));
				preparedRead = session.prepare(
						select);
				preparedRangeReadsPerTable.put(
						tableName,
						preparedRead);
			}
		}

		return new BatchedRangeRead(
				preparedRead,
				this,
				ranges);
	}

	public BatchedRangeRead getBatchedRangeRead(
			final String tableName ) {
		return getBatchedRangeRead(
				tableName,
				new ArrayList<>());
	}

	public RowRead getRowRead(
			final String tableName,
			final byte[] rowIdx ) {
		PreparedStatement preparedRead;
		synchronized (preparedRangeReadsPerTable) {
			preparedRead = preparedRangeReadsPerTable.get(
					tableName);
			if (preparedRead == null) {
				final Select select = getSelect(
						tableName);
				select.where(
						QueryBuilder.eq(
								CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
								QueryBuilder.bindMarker(
										CassandraRow.CassandraField.GW_IDX_KEY.getBindMarkerName())));
				preparedRead = session.prepare(
						select);
				preparedRangeReadsPerTable.put(
						tableName,
						preparedRead);
			}
		}

		return new RowRead(
				preparedRead,
				this,
				rowIdx);

	}

	public RowRead getRowRead(
			final String tableName ) {
		return getRowRead(
				tableName,
				null);
	}

	public CloseableIterator<CassandraRow> executeQuery(
			final Statement... statements ) {
		// first create a list of asynchronous query executions
		final List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(
				statements.length);
		for (final Statement s : statements) {
			futures.add(
					session.executeAsync(
							s));
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

	public Writer createWriter(
			final String tableName,
			final boolean createTable ) {
		final CassandraWriter writer = new CassandraWriter(
				tableName,
				this);
		if (createTable) {
			if (!tableExists(
					tableName)) {
				final Create create = getCreateTable(
						tableName);
				for (final CassandraField f : CassandraField.values()) {
					f.addColumn(
							create);
				}
				executeCreateTable(
						create,
						tableName);
			}
		}
		return writer;
	}

	@Override
	public void deleteAll()
			throws Exception {}

	private static class ListenableFutureToCompletableFuture implements
			Function<ListenableFuture<ResultSet>, CompletableFuture<ResultSet>>
	{
		@Override
		public CompletableFuture<ResultSet> apply(
				final ListenableFuture<ResultSet> input ) {
			return CompletableFuturesExtra.toCompletableFuture(
					input);
		}
	}
}
