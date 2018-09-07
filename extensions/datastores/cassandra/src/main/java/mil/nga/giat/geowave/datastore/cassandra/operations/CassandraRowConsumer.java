package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;

public class CassandraRowConsumer<T> implements
		Iterator<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CassandraRowConsumer.class);
	private Object nextRow = null;
	private final BlockingQueue<Object> blockingQueue;
	protected static final Object POISON = new Object();

	public CassandraRowConsumer(
			final BlockingQueue<Object> blockingQueue ) {
		this.blockingQueue = blockingQueue;
	}

	@Override
	public boolean hasNext() {
		if (nextRow != null) {
			return true;
		}
		else {
			try {
				nextRow = blockingQueue.take();
			}
			catch (final InterruptedException e) {
				LOGGER.warn(
						"Interrupted while waiting on hasNext",
						e);
				return false;
			}
		}
		if (!nextRow.equals(POISON)) {
			return true;
		}
		else {
			try {
				blockingQueue.put(POISON);
			}
			catch (final InterruptedException e) {
				LOGGER.warn(
						"Interrupted while finishing consuming from queue",
						e);
			}
			nextRow = null;
			return false;
		}
	}

	@Override
	public T next() {
		final T retVal = (T) nextRow;
		nextRow = null;
		return retVal;
	}

}
