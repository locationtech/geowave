/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.config;

import java.io.File;
import java.util.Properties;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.rocksdb.RocksDBStoreFactoryFamily;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

public class RocksDBOptions extends StoreFactoryOptions {
  @Parameter(
      names = "--dir",
      description = "The directory to read/write to.  Defaults to \"rocksdb\" in the working directory.")
  private String dir = "rocksdb";
  @Parameter(
      names = "--compactOnWrite",
      description = "Whether to compact on every write, if false it will only compact on merge. Defaults to true",
      arity = 1)
  private boolean compactOnWrite = true;
  @Parameter(
      names = "--batchWriteSize",
      description = "The size (in records) for each batched write. Anything <= 1 will use synchronous single record writes without batching. Defaults to 1000.")
  private int batchWriteSize = 1000;


  @ParametersDelegate
  protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
    @Override
    public boolean isServerSideLibraryEnabled() {
      return false;
    }

    @Override
    protected int defaultMaxRangeDecomposition() {
      return RocksDBUtils.ROCKSDB_DEFAULT_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected int defaultAggregationMaxRangeDecomposition() {
      return RocksDBUtils.ROCKSDB_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
    }

    @Override
    protected boolean defaultEnableVisibility() {
      return false;
    }
  };

  @Override
  public void validatePluginOptions() throws ParameterException {
    // Set the directory to be absolute
    dir = new File(dir).getAbsolutePath();
    super.validatePluginOptions();
  }

  @Override
  public void validatePluginOptions(final Properties properties) throws ParameterException {
    // Set the directory to be absolute
    dir = new File(dir).getAbsolutePath();
    super.validatePluginOptions(properties);
  }

  public RocksDBOptions() {
    super();
  }

  public RocksDBOptions(final String geowaveNamespace) {
    super(geowaveNamespace);
  }

  public boolean isCompactOnWrite() {
    return compactOnWrite;
  }

  public void setCompactOnWrite(final boolean compactOnWrite) {
    this.compactOnWrite = compactOnWrite;
  }

  public void setDirectory(final String dir) {
    this.dir = dir;
  }

  public String getDirectory() {
    return dir;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new RocksDBStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return baseOptions;
  }

  public int getBatchWriteSize() {
    return batchWriteSize;
  }

  public void setBatchWriteSize(final int batchWriteSize) {
    this.batchWriteSize = batchWriteSize;
  }
}
