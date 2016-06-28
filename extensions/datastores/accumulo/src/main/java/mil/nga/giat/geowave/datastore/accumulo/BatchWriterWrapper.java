package mil.nga.giat.geowave.datastore.accumulo;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.Writer;

/**
 * This is a basic wrapper around the Accumulo batch writer so that write
 * operations will use an interface that can be implemented differently for
 * different purposes. For example, a bulk ingest can be performed by replacing
 * this implementation within a custom implementation of AccumuloOperations.
 */
public class BatchWriterWrapper implements
		Writer<Mutation>
{
	private final static Logger LOGGER = Logger.getLogger(BatchWriterWrapper.class);
	private org.apache.accumulo.core.client.BatchWriter batchWriter;

	public BatchWriterWrapper(
			final org.apache.accumulo.core.client.BatchWriter batchWriter ) {
		this.batchWriter = batchWriter;
	}

	public org.apache.accumulo.core.client.BatchWriter getBatchWriter() {
		return batchWriter;
	}

	public void setBatchWriter(
			final org.apache.accumulo.core.client.BatchWriter batchWriter ) {
		this.batchWriter = batchWriter;
	}

	@Override
	public void write(
			final Iterable<Mutation> mutations ) {
		try {
			batchWriter.addMutations(mutations);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to close batch writer",
					e);
		}
	}

	@Override
	public void write(
			final Mutation mutation ) {
		try {
			batchWriter.addMutation(mutation);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to write batch writer",
					e);
		}
	}

	@Override
	public void close() {
		try {
			batchWriter.close();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to close batch writer",
					e);
		}
	}

	@Override
	public void flush() {
		try {
			batchWriter.flush();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to flush batch writer",
					e);
		}
	}

}
