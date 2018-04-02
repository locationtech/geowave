package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;

import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;

public class ServerSideOperationStore
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerSideOperationStore.class);
	private final Map<TableKey, TableOpStore> map = new HashMap<>();

	public ServerSideOperationStore() {

	}

	public void addOperation(
			final String namespace,
			final String qualifier,
			final String opName,
			final int priority,
			final ImmutableSet<ServerOpScope> scopes,
			final byte[] classId,
			final Map<String, String> options ) {
		final TableKey key = new TableKey(
				namespace,
				qualifier);
		TableOpStore tableStore = map.get(key);
		if (tableStore == null) {
			tableStore = new TableOpStore();
			map.put(
					key,
					tableStore);
		}
		tableStore.addOperation(
				opName,
				priority,
				scopes,
				classId,
				options);
	}

	public Collection<HBaseServerOp> getOperations(
			final String namespace,
			final String qualifier,
			final ServerOpScope scope ) {
		final TableOpStore tableStore = map.get(new TableKey(
				namespace,
				qualifier));
		if (tableStore != null) {
			return tableStore.getOperations(scope);
		}
		return Collections.emptyList();
	}

	private static class TableOpStore
	{
		SortedMap<ServerSideOperationKey, ServerSideOperationValue> map = new TreeMap<>();

		private void addOperation(
				final String opName,
				final int priority,
				final ImmutableSet<ServerOpScope> scopes,
				final byte[] classId,
				final Map<String, String> options ) {
			map.put(
					new ServerSideOperationKey(
							opName,
							priority),
					new ServerSideOperationValue(
							scopes,
							classId,
							options));
		}

		private Collection<HBaseServerOp> getOperations(
				final ServerOpScope scope ) {
			return Collections2.filter(
					Collections2.transform(
							map.values(),
							new Function<ServerSideOperationValue, HBaseServerOp>() {
								@Override
								public HBaseServerOp apply(
										final ServerSideOperationValue input ) {
									return input.getOperation(scope);
								}
							}),
					Predicates.notNull());
		}
	}

	private static class ServerSideOperationValue
	{
		private final ImmutableSet<ServerOpScope> scopes;
		private byte[] classId;
		private Map<String, String> options;
		private HBaseServerOp operation;

		public ServerSideOperationValue(
				final ImmutableSet<ServerOpScope> scopes,
				final byte[] classId,
				final Map<String, String> options ) {
			super();
			this.scopes = scopes;
			this.classId = classId;
			this.options = options;
		}

		private HBaseServerOp getOperation(
				final ServerOpScope scope ) {
			if (!scopes.contains(scope)) {
				return null;
			}
			// defer instantiation of the filter until its required
			if (operation == null) {
				operation = createOperation();
			}
			return operation;
		}

		private HBaseServerOp createOperation() {
			final HBaseServerOp op = (HBaseServerOp) PersistenceUtils.fromClassId(classId);
			if (op != null) {
				try {
					op.init(options);
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to initialize operation",
							e);
				}
			}
			return op;
		}
	}

	private static class ServerSideOperationKey implements
			Comparable<ServerSideOperationKey>
	{
		private final String opName;
		private final int priority;

		public ServerSideOperationKey(
				final String opName,
				final int priority ) {
			this.opName = opName;
			this.priority = priority;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((opName == null) ? 0 : opName.hashCode());
			result = (prime * result) + priority;
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final ServerSideOperationKey other = (ServerSideOperationKey) obj;
			if (opName == null) {
				if (other.opName != null) {
					return false;
				}
			}
			else if (!opName.equals(other.opName)) {
				return false;
			}
			if (priority != other.priority) {
				return false;
			}
			return true;
		}

		@Override
		public int compareTo(
				final ServerSideOperationKey o ) {
			int retVal = Integer.compare(
					priority,
					o.priority);
			if (retVal == 0) {
				retVal = opName.compareTo(o.opName);
			}
			return retVal;
		}
	}

	private static class TableKey
	{
		private final String namespace;
		private final String qualifier;

		public TableKey(
				final String namespace,
				final String qualifier ) {
			this.namespace = namespace;
			this.qualifier = qualifier;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((namespace == null) ? 0 : namespace.hashCode());
			result = (prime * result) + ((qualifier == null) ? 0 : qualifier.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final TableKey other = (TableKey) obj;
			if (namespace == null) {
				if (other.namespace != null) {
					return false;
				}
			}
			else if (!namespace.equals(other.namespace)) {
				return false;
			}
			if (qualifier == null) {
				if (other.qualifier != null) {
					return false;
				}
			}
			else if (!qualifier.equals(other.qualifier)) {
				return false;
			}
			return true;
		}
	}
}
