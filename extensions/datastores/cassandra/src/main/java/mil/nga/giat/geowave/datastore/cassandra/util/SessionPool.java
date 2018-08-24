package mil.nga.giat.geowave.datastore.cassandra.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class SessionPool
{

	private static SessionPool singletonInstance;

	public static synchronized SessionPool getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new SessionPool();
		}
		return singletonInstance;
	}

	protected SessionPool() {}

	private final Map<String, Pair<Cluster, Session>> sessionCache = new HashMap<String, Pair<Cluster, Session>>();

	public Session getSession(
			final String contactPoints ) {
		return getPair(
				contactPoints).getRight();
	}

	public synchronized Pair<Cluster, Session> getPair(
			final String contactPoints ) {
		Pair<Cluster, Session> sessionClusterPair = sessionCache
				.get(
						contactPoints);
		if (sessionClusterPair == null) {
			Cluster cluster = Cluster
					.builder()
					.addContactPoints(
							contactPoints
									.split(
											","))
					.build();
			Session session = cluster.connect();
			sessionClusterPair = ImmutablePair
					.of(
							cluster,
							session);
			sessionCache
					.put(
							contactPoints,
							sessionClusterPair);
		}
		return sessionClusterPair;
	}
	public Cluster getCluster(
			final String contactPoints ) {
		return getPair(
				contactPoints).getLeft();
	}
}
