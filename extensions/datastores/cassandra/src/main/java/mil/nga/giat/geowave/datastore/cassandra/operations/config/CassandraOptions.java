package mil.nga.giat.geowave.datastore.cassandra.operations.config;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;

public class CassandraOptions extends
		BaseDataStoreOptions
{
	@Parameter(names = "--batchWriteSize", description = "The number of inserts in a batch write.")
	private int batchWriteSize = 25000;
	
//	@Parameter(names = "--writeThreads", description = "The max number of concurrent threads on write.")
//	private int writeThreads = 16;

	public int getBatchWriteSize() {
		return batchWriteSize;
	}

	public void setBatchWriteSize(
			int batchWriteSize ) {
		this.batchWriteSize = batchWriteSize;
	}
}
