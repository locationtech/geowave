/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.config;

import java.util.Locale;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBStoreFactoryFamily;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;
import software.amazon.awssdk.regions.Region;


public class DynamoDBOptions extends StoreFactoryOptions {
  public static final int DEFAULT_MAX_CONNECTIONS = 50;

  @Parameter(
      names = "--endpoint",
      description = "The endpoint to connect to(specify either endpoint/region not both) ",
      required = false)
  protected String endpoint;

  @Parameter(
      names = "--region",
      description = "The AWS region to use(specify either endpoint/region not both)",
      converter = RegionConverter.class)
  protected Region region = null;

  @Parameter(
      names = "--initialWriteCapacity",
      description = "The maximum number of writes consumed per second before throttling occurs")
  protected long writeCapacity = 5;

  @Parameter(
      names = "--initialReadCapacity",
      description = "The maximum number of strongly consistent reads consumed per second before throttling occurs")
  protected long readCapacity = 5;

  /** List of client configuration that the user can tweak */
  @Parameter(
      names = "--maxConnections",
      description = "The maximum number of open http(s) connections active at any given time")
  protected int maxConnections = DEFAULT_MAX_CONNECTIONS;

  @Parameter(
      names = "--protocol",
      description = "The protocol to use. HTTP or HTTPS",
      converter = ProtocolConverter.class)
  protected Protocol protocol = Protocol.HTTPS;

  /** Kept so that existing commands and stored configurations still parse. */
  @Parameter(
      names = "--cacheResponseMetadata",
      hidden = true,
      description = "No longer has any effect: AWS SDK for Java 2.x keeps no response metadata cache")
  protected boolean enableCacheResponseMetadata = true;

  // End of client configuration parameters

  @ParametersDelegate
  protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
    @Override
    public boolean isServerSideLibraryEnabled() {
      return false;
    }

    @Override
    protected boolean defaultEnableVisibility() {
      return false;
    }

    @Override
    protected int defaultDataIndexBatchSize() {
      return 100;
    }

    @Override
    protected int defaultMaxRangeDecomposition() {
      return 200;
    }
  };

  public DynamoDBOptions() {}

  public DynamoDBOptions(
      final String endpoint,
      final Region region,
      final long writeCapacity,
      final long readCapacity,
      final int maxConnections,
      final Protocol protocol,
      final boolean enableCacheResponseMetadata,
      final String gwNamespace,
      final BaseDataStoreOptions baseOptions) {
    super(gwNamespace);
    this.endpoint = endpoint;
    this.region = region;
    this.writeCapacity = writeCapacity;
    this.readCapacity = readCapacity;
    this.maxConnections = maxConnections;
    this.protocol = protocol;
    this.enableCacheResponseMetadata = enableCacheResponseMetadata;
    this.baseOptions = baseOptions;
  }

  public void setRegion(final Region region) {
    this.region = region;
  }

  public Region getRegion() {
    return region;
  }

  public void setEndpoint(final String endpoint) {
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

  public void setWriteCapacity(final long writeCapacity) {
    this.writeCapacity = writeCapacity;
  }

  public long getReadCapacity() {
    return readCapacity;
  }

  public void setReadCapacity(final long readCapacity) {
    this.readCapacity = readCapacity;
  }

  public void setEnableCacheResponseMetadata(final boolean enableCacheResponseMetadata) {
    this.enableCacheResponseMetadata = enableCacheResponseMetadata;
  }

  public boolean isEnableCacheResponseMetadata() {
    return enableCacheResponseMetadata;
  }

  public void setProtocol(final Protocol protocol) {
    this.protocol = protocol;
  }

  public Protocol getProtocol() {
    return this.protocol;
  }

  public void setMaxConnections(final int maxConnections) {
    this.maxConnections = maxConnections;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new DynamoDBStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return baseOptions;
  }

  public enum Protocol {
    HTTP, HTTPS;

    public String scheme() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  /**
   * Jcommander helper class for AWS Region. Configurations saved by GeoWave 2.x hold the name of
   * the SDK v1 Regions constant, such as US_EAST_1, rather than the region ID.
   */
  public static class RegionConverter implements IStringConverter<Region> {

    @Override
    public Region convert(final String regionName) {
      if (regionName == null || regionName.isEmpty()) {
        return null;
      }
      return Region.of(regionName.toLowerCase(Locale.ROOT).replaceAll("_", "-"));
    }
  }


  /** JCommander helper class for Protocol */
  public static class ProtocolConverter implements IStringConverter<Protocol> {

    @Override
    public Protocol convert(final String protocolName) {
      final String protocolUpperCase = protocolName.toUpperCase();
      if (!protocolUpperCase.equals("HTTP") && !protocolUpperCase.equals("HTTPS")) {
        throw new ParameterException(
            "Value "
                + protocolName
                + "can not be converted to Protocol. "
                + "Available values are: http and https.");
      }

      return Protocol.valueOf(protocolUpperCase);
    }
  }
}
