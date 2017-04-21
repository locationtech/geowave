package mil.nga.giat.geowave.datastore.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.regions.Regions;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

/**
 * Jcommander helper class for AWS Region
 *
 */
class RegionConvertor implements
		IStringConverter<Regions>
{

	@Override
	public Regions convert(
			String regionName ) {
		return Regions.fromName(regionName);
	}

}

/**
 * JCommander helper class for Protocol
 *
 */
class ProtocolConvertor implements
		IStringConverter<Protocol>
{

	@Override
	public Protocol convert(
			String protocolName ) {
		String protocolLowerCase = protocolName.toLowerCase();
		if (!protocolLowerCase.equals("http") && !protocolLowerCase.equals("https"))
			throw new ParameterException(
					"Value " + protocolName + "can not be converted to Protocol. "
							+ "Available values are: http and https.");

		return Protocol.valueOf(protocolLowerCase);
	}

}

public class DynamoDBOptions extends
		StoreFactoryOptions
{
	@Parameter(names = "--endpoint", description = "The endpoint to connect to(specify either endpoint/region not both) ", required = false)
	protected String endpoint;

	@Parameter(names = "--region", description = "The AWS region to use(specify either endpoint/region not both)")
	protected Regions region = null;

	@Parameter(names = "--initialWriteCapacity")
	protected long writeCapacity = 5;
	@Parameter(names = "--initialReadCapacity")
	protected long readCapacity = 5;

	/**
	 * List of client configuration that the user can tweak
	 */

	@Parameter(names = "--maxConnections", description = "The maximum number of open http(s) connections"
			+ " active at any given time")
	protected int maxConnections = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

	@Parameter(names = "--protocol", description = "The protocol to use. HTTP or HTTPS")
	protected Protocol protocol = Protocol.HTTPS;

	@Parameter(names = "--cacheResponseMetadata", description = "Whether to cache responses from aws(true or false). "
			+ "High performance systems can disable this but debugging will be more difficult")
	protected boolean enableCacheResponseMetadata = ClientConfiguration.DEFAULT_CACHE_RESPONSE_METADATA;

	// End of client configuration parameters

	@ParametersDelegate
	protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions();

	private ClientConfiguration clientConfig = new ClientConfiguration();

	public ClientConfiguration getClientConfig() {
		clientConfig.setCacheResponseMetadata(enableCacheResponseMetadata);
		clientConfig.setProtocol(protocol);
		clientConfig.setMaxConnections(maxConnections);
		return clientConfig;
	}

	public void setRegion(
			final Regions region ) {
		this.region = region;
	}

	public Regions getRegion() {
		return region;
	}

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
