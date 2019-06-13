/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.config;

import java.util.function.Function;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.redis.RedisStoreFactoryFamily;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.client.codec.Codec;
import org.redisson.codec.LZ4Codec;
import org.redisson.codec.SnappyCodec;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class RedisOptions extends StoreFactoryOptions {
  @Parameter(
      names = {"--address", "-a"},
      required = true,
      description = "The address to connect to, such as redis://127.0.0.1:6379")
  private String address;

  @Parameter(
      names = "--compression",
      description = "Can be \"snappy\",\"lz4\", or \"none\". Defaults to snappy.",
      converter = CompressionConverter.class)
  private Compression compression = Compression.SNAPPY;

  @ParametersDelegate
  protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
    @Override
    public boolean isServerSideLibraryEnabled() {
      return false;
    }

    @Override
    protected int defaultMaxRangeDecomposition() {
      return RedisUtils.REDIS_DEFAULT_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected int defaultAggregationMaxRangeDecomposition() {
      return RedisUtils.REDIS_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected boolean defaultEnableVisibility() {
      return false;
    }
  };

  public RedisOptions() {
    super();
  }

  public RedisOptions(final String geowaveNamespace) {
    super(geowaveNamespace);
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new RedisStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return baseOptions;
  }

  public void setAddress(final String address) {
    this.address = address;
  }

  public void setCompression(final Compression compression) {
    this.compression = compression;
  }

  public String getAddress() {
    return address;
  }

  public Compression getCompression() {
    return compression;
  }

  public static enum Compression {
    SNAPPY(c -> new SnappyCodec(c)), L4Z(c -> new LZ4Codec(c)), NONE(c -> c);
    private Function<Codec, Codec> compressionTransform;

    private Compression(final Function<Codec, Codec> compressionTransform) {
      this.compressionTransform = compressionTransform;
    }

    public Codec getCodec(final Codec innerCodec) {
      return compressionTransform.apply(innerCodec);
    }
  };

  public static class CompressionConverter implements IStringConverter<Compression> {

    @Override
    public Compression convert(final String value) {
      return Compression.valueOf(value.toUpperCase());
    }
  }
}
