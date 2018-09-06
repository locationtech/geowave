package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class BatchHandler
{
	protected final Session session;
	private Type type = Type.UNLOGGED;
	protected final Map<ByteArrayId, BatchStatement> batches = new HashMap<>();

	public BatchHandler(
			final Session session ) {
		this.session = session;
	}

	protected BatchStatement addStatement(
			final GeoWaveRow row, 
			final Statement statement ) {
		ByteArrayId partition = new ByteArrayId(row.getPartitionKey());
		BatchStatement tokenBatch = batches.get(partition);

		if (tokenBatch == null) {
			tokenBatch = new BatchStatement(
					type);

			batches.put(
					partition,
					tokenBatch);
		}
		synchronized (tokenBatch) {
			tokenBatch.add(statement);
		}
		return tokenBatch;
	}
}
