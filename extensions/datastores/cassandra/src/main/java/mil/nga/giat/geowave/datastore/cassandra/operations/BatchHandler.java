package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class BatchHandler
{
	protected final Session session;
	private static final int RF = 3;
	private Type type;
	protected final Map<Set<Host>, BatchStatement> batches = new HashMap<>();

	public BatchHandler(
			final Session session ) {
		this.session = session;
	}

	protected BatchStatement addStatement(
			final Statement statement ) {
		final Set<Host> hosts = new HashSet<>();
		int replicas = 0;
		final Iterator<Host> it = session
				.getCluster()
				.getConfiguration()
				.getPolicies()
				.getLoadBalancingPolicy()
				.newQueryPlan(
						statement.getKeyspace(),
						statement);

		while (it.hasNext() && (replicas < RF)) {
			hosts.add(
					it.next());
			replicas++;
		}

		BatchStatement tokenBatch = batches.get(
				hosts);

		if (tokenBatch == null) {
			tokenBatch = new BatchStatement(
					type);

			batches.put(
					hosts,
					tokenBatch);
		}
		synchronized (tokenBatch) {
			tokenBatch.add(
					statement);
		}
		return tokenBatch;
	}
}
