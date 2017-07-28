package mil.nga.giat.geowave.datastore.hbase.server;

class ServerSideOperationKey implements
		Comparable<ServerSideOperationKey>
{
	private final String namespace;
	private final String qualifier;
	private final String opName;
	private final int priority;

	public ServerSideOperationKey(
			final String namespace,
			final String qualifier,
			final String opName,
			final int priority ) {
		this.namespace = namespace;
		this.qualifier = qualifier;
		this.opName = opName;
		this.priority = priority;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((namespace == null) ? 0 : namespace.hashCode());
		result = (prime * result) + ((opName == null) ? 0 : opName.hashCode());
		result = (prime * result) + priority;
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
		final ServerSideOperationKey other = (ServerSideOperationKey) obj;
		if (namespace == null) {
			if (other.namespace != null) {
				return false;
			}
		}
		else if (!namespace.equals(other.namespace)) {
			return false;
		}
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

	@Override
	public int compareTo(
			final ServerSideOperationKey o ) {
		int retVal = Integer.compare(
				priority,
				o.priority);
		if (retVal == 0) {
			retVal = namespace.compareTo(o.namespace);
			if (retVal == 0) {
				retVal = qualifier.compareTo(o.qualifier);
				if (retVal == 0) {
					retVal = opName.compareTo(o.opName);
				}
			}
		}
		return retVal;
	}
}