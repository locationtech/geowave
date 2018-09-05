package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
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
	private static final boolean BATCH = false;
	private final int batchSize;
	private final PreparedStatement preparedInsert;
	private final static int MAX_CONCURRENT_WRITE = 100;
	// only allow so many outstanding async reads or writes, use this semaphore
	// to control it
	private final Semaphore writeSemaphore = new Semaphore(
			MAX_CONCURRENT_WRITE);

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
			if (BATCH) {
				final BatchStatement currentBatch = addStatement(statement);
				synchronized (currentBatch) {
					if (currentBatch.size() >= batchSize) {
						writeBatch(currentBatch);
					}
				}
			}
			else {
				executeAsync(statement);
			}
		}
		else {
			session.execute(statement);
		}
	}

	private void writeBatch(
			final BatchStatement batch ) {
		try {
			writeSemaphore.acquire();
			executeAsync(batch);

			batch.clear();
		}
		catch (InterruptedException e) {
			LOGGER.warn(
					"async write semaphore interupted",
					e);
			writeSemaphore.release();
		}
	}

	private void executeAsync(
			Statement statement ) {
		final ResultSetFuture future = session.executeAsync(statement);
		Futures.addCallback(
				future,
				new IngestCallback(
						writeSemaphore),
				CassandraOperations.WRITE_RESPONSE_THREADS);
	}

	@Override
	public void close()
			throws Exception {
		for (final BatchStatement batch : batches.values()) {
			synchronized (batch) {
				writeBatch(batch);
			}
		}

		// need to wait for all asynchronous batches to finish writing
		// before exiting close() method
		writeSemaphore.acquire(MAX_CONCURRENT_WRITE);
	}

	// callback class
	protected static class IngestCallback implements
			FutureCallback<ResultSet>
	{

		private final Semaphore semaphore;

		public IngestCallback(
				Semaphore semaphore ) {
			this.semaphore = semaphore;
		}

		@Override
		public void onSuccess(
				final ResultSet result ) {
			semaphore.release();
			// placeholder: put any logging or on success logic here.
		}

		@Override
		public void onFailure(
				final Throwable t ) {
			semaphore.release();
			// go ahead and wrap in a runtime exception for this case, but you
			// can do logging or start counting errors.
			throw new RuntimeException(
					t);
		}
	}
}
