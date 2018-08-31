package mil.nga.giat.geowave.datastore.cassandra.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.datastore.cassandra.util.CassandraUtils;

public class BatchedWrite extends
		BatchHandler implements
		AutoCloseable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BatchedWrite.class);
	// TODO: default batch size is tiny at 50 KB, reading recommendations re:
	// micro-batch writing
	// (https://dzone.com/articles/efficient-cassandra-write), we should be able
	// to gain some efficiencies for bulk ingests with batches if done
	// correctly, while other recommendations contradict this article and
	// suggest don't use batching as a performance optimization
	private static final boolean ASYNC = true;
	private static int MAX_ASYNC_WRITES = 100;
	private final int batchSize;
	private final PreparedStatement preparedInsert;
	private static int asyncWritesInProgress = 0;
	private static Object MUTEX = new Object();

	public BatchedWrite(
			final Session session,
			final PreparedStatement preparedInsert,
			final int batchSize ) {
		super(
				session);
		this.preparedInsert = preparedInsert;
		this.batchSize = batchSize;
	}

	public void insert(
			final GeoWaveRow row ) {
		final BoundStatement[] statements = CassandraUtils.bindInsertion(
				preparedInsert,
				row);
		for (final BoundStatement statement : statements) {
			insertStatement(statement);
		}
	}

	private void insertStatement(
			final BoundStatement statement ) {
		if (ASYNC) {
			final BatchStatement currentBatch = addStatement(statement);
			synchronized (currentBatch) {
				if (currentBatch.size() >= batchSize) {
					writeBatch(currentBatch);
				}
			}
		}
		else {
			session.execute(statement);
		}
	}

	private void waitForWritesToFinish(
			int maxWrites ) {

		while (asyncWritesInProgress >= maxWrites) {
			try {
				// TODO eventually better to change to wait/notify
				// for now just sleep
				Thread.sleep(200L);
			}
			catch (InterruptedException e) {
				LOGGER.warn(
						"unable to wait for async writes in progress",
						e);
			}
		}
	}

	private void writeBatch(
			final BatchStatement batch ) {
		synchronized (MUTEX) {
			waitForWritesToFinish(MAX_ASYNC_WRITES);
			++asyncWritesInProgress;
		}
		final ResultSetFuture future = session.executeAsync(batch);
		Futures.addCallback(
				future,
				new IngestCallback(),
				CassandraOperations.WRITE_RESPONSE_THREADS);
		batch.clear();
	}

	@Override
	public void close()
			throws Exception {
		for (final BatchStatement batch : batches.values()) {
			synchronized (batch) {
				writeBatch(batch);
			}
		}
		waitForWritesToFinish(0);
		// TODO need to wait for all asynchronous batches to finish writing
		// before exiting close() method
	}

	// callback class
	protected static class IngestCallback implements
			FutureCallback<ResultSet>
	{

		@Override
		public void onSuccess(
				final ResultSet result ) {

			synchronized (MUTEX) {
				asyncWritesInProgress--;
			}
			// placeholder: put any logging or on success logic here.
		}

		@Override
		public void onFailure(
				final Throwable t ) {

			synchronized (MUTEX) {
				asyncWritesInProgress--;
			}
			// go ahead and wrap in a runtime exception for this case, but you
			// can do logging or start counting errors.
			t.printStackTrace();
			throw new RuntimeException(
					t);
		}
	}
}
