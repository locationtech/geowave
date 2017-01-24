package mil.nga.giat.geowave.datastore.cassandra.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.PreparedStatement;

public class KeyspaceStatePool
{
	private static KeyspaceStatePool singletonInstance;

	public static synchronized KeyspaceStatePool getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new KeyspaceStatePool();
		}
		return singletonInstance;
	}

	private final Map<Pair<String, String>, KeyspaceState> keyspaceStateCache = new HashMap<Pair<String, String>, KeyspaceState>();

	protected KeyspaceStatePool() {}

	public synchronized KeyspaceState getCachedState(
			final String contactPoints,
			final String keyspace ) {

		final Pair<String, String> key = ImmutablePair.of(
				contactPoints,
				keyspace);
		KeyspaceState state = keyspaceStateCache.get(key);
		if (state == null) {
			state = new KeyspaceState();
			keyspaceStateCache.put(
					key,
					state);
		}
		return state;
	}

	public static class KeyspaceState
	{
		public final Map<String, PreparedStatement> preparedRangeReadsPerTable = new HashMap<>();
		public final Map<String, PreparedStatement> preparedRowReadPerTable = new HashMap<>();
		public final Map<String, PreparedStatement> preparedWritesPerTable = new HashMap<>();
		public final Map<String, Boolean> tableExistsCache = new HashMap<>();
	}

}
