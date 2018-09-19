/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.datastore.dynamodb;

import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.regions.Regions;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

/**
 * Jcommander helper class for AWS Region
 *
 */
class RegionConvertor implements
		IStringConverter<Regions>
{

	@Override
	public Regions convert(
			final String regionName ) {
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
			final String protocolName ) {
		final String protocolLowerCase = protocolName.toLowerCase();
		if (!protocolLowerCase.equals("http") && !protocolLowerCase.equals("https")) {
			throw new ParameterException(
					"Value " + protocolName + "can not be converted to Protocol. "
							+ "Available values are: http and https.");
		}

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
	protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
		@Override
		public boolean isServerSideLibraryEnabled() {
			return false;
		}
	};

	public DynamoDBOptions() {}

	public DynamoDBOptions(
			String endpoint,
			Regions region,
			long writeCapacity,
			long readCapacity,
			int maxConnections,
			Protocol protocol,
			boolean enableCacheResponseMetadata,
			String gwNamespace,
			BaseDataStoreOptions baseOptions ) {
		super(
				gwNamespace);
		this.endpoint = endpoint;
		this.region = region;
		this.writeCapacity = writeCapacity;
		this.readCapacity = readCapacity;
		this.maxConnections = maxConnections;
		this.protocol = protocol;
		this.enableCacheResponseMetadata = enableCacheResponseMetadata;
		this.baseOptions = baseOptions;
	}

	private final ClientConfiguration clientConfig = new ClientConfiguration();

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

	@Override
	public DataStoreOptions getStoreOptions() {
		return baseOptions;
	}

}
