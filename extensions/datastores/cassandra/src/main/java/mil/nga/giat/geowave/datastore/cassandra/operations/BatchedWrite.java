package mil.nga.giat.geowave.datastore.cassandra.operations;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;

public class BatchedWrite extends
		BatchHandler implements
		AutoCloseable
{
	// TODO: default batch size is tiny at 50 KB, reading recommendations re:
	// micro-batch writing
	// (https://dzone.com/articles/efficient-cassandra-write), we should be able
	// to gain some efficiencies for bulk ingests with batches if done
	// correctly, while other recommendations contradict this article and
	// suggest don't use batching as a performance optimization
	private static final boolean ASYNC = false;
	private final int batchSize;
	private final PreparedStatement preparedInsert;

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
			final CassandraRow row ) {
		if (ASYNC) {
			final BatchStatement currentBatch = addStatement(row.bindInsertion(preparedInsert));
			synchronized (currentBatch) {
				if (currentBatch.size() >= batchSize) {
					writeBatch(currentBatch);
				}
			}
		}
		else {
			session.execute(row.bindInsertion(preparedInsert));
		}
	}

	private void writeBatch(
			final BatchStatement batch ) {
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
