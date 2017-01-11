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
		// final BatchStatement currentBatch = addStatement(
		// row.bindInsertion(
		// preparedInsert));
		// synchronized (currentBatch) {
		// if (currentBatch.size() >= batchSize) {
		// writeBatch(
		// currentBatch);
		// }
		// }
		session.execute(row.bindInsertion(preparedInsert));
	}

	private void writeBatch(
			final BatchStatement batch ) {
		final ResultSet future = session.execute(batch);
		// Futures.addCallback(
		// future,
		// new IngestCallback(),
		// CassandraOperations.WRITE_RESPONSE_THREADS);
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
