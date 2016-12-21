package mil.nga.giat.geowave.datastore.dynamodb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

public class DynamoDBOptions extends
		StoreFactoryOptions
{
	@Parameter(names = "--endpoint", required = true)
	protected String endpoint;

	@Parameter(names = "--initialWriteCapacity")
	protected long writeCapacity = 10;
	@Parameter(names = "--initialReadCapacity")
	protected long readCapacity = 10;

	@ParametersDelegate
	protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions();

	public void setEndpoint(
			final String endpoint ) {
		this.endpoint = endpoint;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public DataStoreOptions getBaseOptions() {
		return baseOptions;
	}

	public long getWriteCapacity() {
		return writeCapacity;
	}

	public void setWriteCapacity(
			final long writeCapacity ) {
		this.writeCapacity = writeCapacity;
	}

	public long getReadCapacity() {
		return readCapacity;
	}

	public void setReadCapacity(
			final long readCapacity ) {
		this.readCapacity = readCapacity;
	}

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new DynamoDBStoreFactoryFamily();
	}

}
