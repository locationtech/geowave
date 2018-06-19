package mil.nga.giat.geowave.datastore.cassandra.operations.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.cassandra.CassandraStoreFactoryFamily;

public class CassandraRequiredOptions extends
		StoreFactoryOptions
{
	@Parameter(names = "--contactPoints", required = true, description = "A single contact point or a comma delimited set of contact points to connect to the Cassandra cluster.")
	private String contactPoints;

	@ParametersDelegate
	private final CassandraOptions additionalOptions = new CassandraOptions();

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new CassandraStoreFactoryFamily();
	}

	public String getContactPoint() {
		return contactPoints;
	}

	public void setContactPoint(
			final String contactPoints ) {
		this.contactPoints = contactPoints;
	}

	@Override
	public DataStoreOptions getStoreOptions() {
		return additionalOptions;
	}
}
