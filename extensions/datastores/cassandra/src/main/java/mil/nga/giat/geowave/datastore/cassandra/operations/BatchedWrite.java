package mil.nga.giat.geowave.datastore.cassandra.operations;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class BatchedWrite extends
		BatchHandler implements
		AutoCloseable
{
	private final int batchSize;

	public BatchedWrite(
			final Session session,
			final int batchSize ) {
		super(
				session);
		this.batchSize = batchSize;
	}

	public void insert(
			final BoundStatement insert ) {
		final BatchStatement currentBatch = addStatement(
				insert);
		synchronized (currentBatch) {
			if (currentBatch.size() >= batchSize) {
				writeBatch(
						currentBatch);
			}
		}
	}

	private void writeBatch(
			final BatchStatement batch ) {
		final ResultSetFuture future = session.executeAsync(
				batch);
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
				writeBatch(
						batch);
			}
		}
	}

	// callback class
	private static class IngestCallback implements
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
			throw new RuntimeException(
					t);
		}
	}
}
