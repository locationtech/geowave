package mil.nga.giat.geowave.datastore.cassandra.util;

import java.util.HashMap;
import java.util.Map;

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

	private final Map<String, Session> sessionCache = new HashMap<String, Session>();

	public synchronized Session getSession(
			final String contactPoints ) {
		Session session = sessionCache.get(
				contactPoints);
		if (session == null) {
			session = Cluster
					.builder()
					.addContactPoints(
							contactPoints.split(
									","))
					.build()
					.newSession();
			sessionCache.put(
					contactPoints,
					session);
		}
		return session;
	}
}
