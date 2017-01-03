package mil.nga.giat.geowave.datastore.cassandra.operations.config;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;

public class CassandraOptions extends
		BaseDataStoreOptions
{
	@Parameter(names = "--batchWriteSize", description = "The number of inserts in a batch write.")
	private int batchWriteSize = 25000;
	@Parameter(names = "--durableWrites", description = "Whether to write to commit log for durability, configured only on creation of new keyspace.", arity = 1)
	private boolean durableWrites = true;
	@Parameter(names = "--replicas", description = "The number of replicas to use when creating a new keyspace.")
	private int replicationFactor = 3;

	public int getBatchWriteSize() {
		return batchWriteSize;
	}

	public void setBatchWriteSize(
			final int batchWriteSize ) {
		this.batchWriteSize = batchWriteSize;
	}

	public boolean isDurableWrites() {
		return durableWrites;
	}

	public void setDurableWrites(
			final boolean durableWrites ) {
		this.durableWrites = durableWrites;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(
			final int replicationFactor ) {
		this.replicationFactor = replicationFactor;
	}
}
